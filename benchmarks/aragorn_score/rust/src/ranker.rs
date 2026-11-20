//! Rust port of the ARAGORN ranker in `workers/aragorn_score/worker.py`.
//!
//! Benchmark only: this exists to measure what a Rust scorer would buy us.
//! It follows the Python `Ranker` (BLENDED profile, which is the only one the
//! worker uses) path by path, including how a failing answer is handled. The
//! deliberate differences are listed in the README; none changes which edges
//! or weights go into a score, only float rounding in the last bits:
//!
//! * r-graph node order is insertion order here, Python's is `set` (hash)
//!   order, so the Laplacian's rows come out permuted;
//! * `ln`, `exp`, row sums and the SVD come from Rust (the SVD from faer),
//!   not numpy/LAPACK.
//!
//! Inputs Python would choke on in odd ways (a list where a string belongs,
//! say) are reported as an error for that answer, which, as in Python, scores
//! the failing analysis and every later one 0.

use std::cell::RefCell;
use std::rc::Rc;

use indexmap::IndexSet as StdIndexSet;
// nalgebra is only the matrix container here; the SVD is faer's (see kirchhoff).
use nalgebra::DMatrix;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

/// Insertion-ordered set with a fast (non-DoS-resistant) hash.
type IndexSet<T> = StdIndexSet<T, FxBuildHasher>;
use serde_json::{Map, Value};

pub type R<T> = Result<T, String>;

const TOTAL_PUBS: f64 = 27_840_000.0;
const LITERATURE_PREDICATE: &str = "biolink:occurs_together_in_literature_with";
const SUPPORT_GRAPHS_ATTRIBUTE: &str = "biolink:support_graphs";

// --- BLENDED profile ------------------------------------------------------

#[derive(Clone, Copy)]
struct Sigmoid {
    lower: f64,
    upper: f64,
    midpoint: f64,
    rate: f64,
}

const fn sig(lower: f64, upper: f64, midpoint: f64, rate: f64) -> Sigmoid {
    Sigmoid {
        lower,
        upper,
        midpoint,
        rate,
    }
}

type SigTable = &'static [(&'static str, Sigmoid)];
type WeightTable = &'static [(&'static str, f64)];

static P_VALUE_SIG: SigTable = &[("p_value", sig(0.0, 1.0, 0.05, -150.0))];
static UNKNOWN_SOURCE_SIG: SigTable = &[
    ("publications", sig(-1.0, 1.0, 0.0, 0.574213221)),
    ("literature_co-occurrence", sig(-1.0, 1.0, 0.0, 0.001373265360835)),
    ("affinity", sig(-1.0, 1.0, 0.0, 0.4)),
    ("unknown_property", sig(0.0, 0.0, 0.0, 0.0)),
];
/// `unknown_source_transformation["unknown_property"]`: the fallback for a
/// property a (known or unknown) source has no transformation for.
const UNKNOWN_PROPERTY_SIG: Sigmoid = sig(0.0, 0.0, 0.0, 0.0);

fn source_transformation(source: &str) -> SigTable {
    static OMNICORP: SigTable = &[(
        "literature_co-occurrence",
        sig(-1.0, 1.0, 0.0, 0.00033),
    )];
    static TMKP: SigTable = &[
        ("publications", sig(-1.0, 1.0, 0.0, 4e-4)),
        ("confidence_score", sig(-1.0, 1.0, 0.0, 3.0)),
    ];
    match source {
        "infores:omnicorp" => OMNICORP,
        "infores:text-mining-provider-targeted" => TMKP,
        "infores:genetics-data-provider" | "infores:icees-kg" | "infores:gwas-catalog" => {
            P_VALUE_SIG
        }
        _ => UNKNOWN_SOURCE_SIG,
    }
}

fn source_weight(source: &str, property: &str) -> f64 {
    static OMNICORP: WeightTable = &[("literature_co-occurrence", 1.0)];
    static TMKP: WeightTable = &[("publications", 0.5), ("confidence_score", 1.0)];
    static P_VALUE: WeightTable = &[("p_value", 1.0)];
    static UNKNOWN: WeightTable = &[
        ("publications", 1.0),
        ("literature_co-occurrence", 1.0),
        ("affinity", 1.0),
        ("unknown_property", 0.0),
    ];
    let table = match source {
        "infores:omnicorp" => OMNICORP,
        "infores:text-mining-provider-targeted" => TMKP,
        "infores:genetics-data-provider" | "infores:icees-kg" | "infores:gwas-catalog" => P_VALUE,
        _ => UNKNOWN,
    };
    table
        .iter()
        .find(|(p, _)| *p == property)
        .map(|(_, w)| *w)
        // unknown_source_weight["unknown_property"]
        .unwrap_or(0.0)
}

fn base_weight(source: &str) -> f64 {
    match source {
        "infores:omnicorp" => 0.0,
        "infores:drugcentral" => 5e-2,
        "infores:hetionet" => 3e-2,
        "infores:text-mining-provider-targeted" => 5e-3,
        "infores:icees-kg" => 3e-2,
        "infores:gwas-catalog" => 3e-2,
        _ => 1e-2,
    }
}

/// `get_source_sigmoid`, including Python's `math.exp` OverflowError.
fn source_sigmoid(value: f64, source: &str, property: &str) -> R<f64> {
    let p = source_transformation(source)
        .iter()
        .find(|(name, _)| *name == property)
        .map(|(_, s)| *s)
        .unwrap_or(UNKNOWN_PROPERTY_SIG);
    let x = -1.0 * p.rate * (value - p.midpoint);
    let e = x.exp();
    if e.is_infinite() && x.is_finite() {
        return Err("math range error (exp overflow in sigmoid)".into());
    }
    Ok(p.lower + ((p.upper - p.lower) / (1.0 + e)))
}

// --- Python value semantics ------------------------------------------------

/// Python truthiness of a JSON value.
pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|x| x != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// A value Python can do arithmetic on (int, float, bool).
fn as_num(v: &Value) -> R<f64> {
    match v {
        Value::Number(n) => n.as_f64().ok_or_else(|| "bad number".to_string()),
        Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        other => Err(format!("unsupported operand type: {}", type_name(other))),
    }
}

/// Python `int(v)`, with ValueError mapped to 0 (as every caller does) and
/// TypeError / OverflowError returned as errors.
fn py_int_or_zero(v: &Value) -> R<f64> {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i as f64)
            } else if let Some(u) = n.as_u64() {
                Ok(u as f64)
            } else {
                let f = n.as_f64().unwrap_or(f64::NAN);
                if f.is_nan() {
                    Ok(0.0) // ValueError
                } else if f.is_infinite() {
                    Err("cannot convert float infinity to integer".into())
                } else {
                    Ok(f.trunc())
                }
            }
        }
        Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        Value::String(s) => Ok(s.trim().parse::<i64>().map(|i| i as f64).unwrap_or(0.0)),
        other => Err(format!("int() argument must not be {}", type_name(other))),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "None",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

/// `obj.get(key, [])` iterated as a list: missing -> empty, list -> items,
/// anything else (None included) -> TypeError.
fn list_or_empty<'a>(obj: &'a Map<String, Value>, key: &str) -> R<&'a [Value]> {
    match obj.get(key) {
        None => Ok(&[]),
        Some(Value::Array(a)) => Ok(a),
        Some(other) => Err(format!("'{key}' is a {}, not a list", type_name(other))),
    }
}

/// `obj.get(key) or []` iterated as a list.
fn list_or_falsy<'a>(obj: &'a Map<String, Value>, key: &str) -> R<&'a [Value]> {
    match obj.get(key) {
        Some(Value::Array(a)) => Ok(a),
        Some(v) if truthy(v) => Err(format!("'{key}' is a {}, not a list", type_name(v))),
        _ => Ok(&[]),
    }
}

/// `x.get(key, "")` followed by `if not x: x = ""`, used as a string.
fn str_or_empty<'a>(obj: &'a Map<String, Value>, key: &str) -> R<&'a str> {
    match obj.get(key) {
        Some(Value::String(s)) => Ok(s),
        Some(v) if truthy(v) => Err(format!("'{key}' is a {}, not a str", type_name(v))),
        _ => Ok(""),
    }
}

fn as_id(v: &Value) -> R<&str> {
    v.as_str()
        .ok_or_else(|| format!("expected an id string, got {}", type_name(v)))
}

/// `shepherd_utils.trapi.binding_ids`.
fn binding_ids(binding: &Value) -> R<Vec<&str>> {
    match binding.as_object() {
        None => Ok(vec![]),
        Some(b) => list_or_falsy(b, "ids")?.iter().map(as_id).collect(),
    }
}

/// `shepherd_utils.trapi.edge_support_graphs`.
fn edge_support_graphs(edge: &Map<String, Value>) -> R<Vec<&str>> {
    let mut out = vec![];
    for attribute in list_or_falsy(edge, "attributes")? {
        let attribute = attribute
            .as_object()
            .ok_or("an attribute is not an object")?;
        if attribute.get("attribute_type_id").and_then(Value::as_str)
            == Some(SUPPORT_GRAPHS_ATTRIBUTE)
        {
            for id in list_or_falsy(attribute, "value")? {
                out.push(as_id(id)?);
            }
        }
    }
    Ok(out)
}

fn empty_map() -> &'static Map<String, Value> {
    static EMPTY: std::sync::OnceLock<Map<String, Value>> = std::sync::OnceLock::new();
    EMPTY.get_or_init(Map::new)
}

// --- Ranker ---------------------------------------------------------------

#[derive(Default)]
struct Closure<'a> {
    nodes: IndexSet<&'a str>,
    edges: IndexSet<&'a str>,
}

type ClosureRef<'a> = Rc<RefCell<Closure<'a>>>;

/// One analysis's r_graph: node 0..n_q are the answer's q_node bindings, the
/// rest are its support nodes. Edges are (r_subject, r_object, k_edge_id).
struct RGraph<'a, 'b> {
    qnodes: Vec<&'b str>,
    n_nodes: usize,
    edges: IndexSet<(u32, u32, &'a str)>,
}

/// The outcome for one answer, before it is written back.
pub enum Outcome {
    /// Every analysis scored (`Value` is what Python writes: a float, or the
    /// int 0 for a degenerate Laplacian).
    Scored(Vec<Value>),
    /// Analyses before `failed_at` got these scores; it and every later one
    /// keep any score they already had, else get 0 (Python's `setdefault`).
    Failed {
        scores: Vec<Value>,
        failed_at: usize,
        error: String,
    },
}

/// A JSON object's entries, re-indexed with a fast hash (serde_json's map
/// hashes with SipHash). Keys are the message's own strings.
struct Index<'a>(FxHashMap<&'a str, &'a Value>);

impl<'a> Index<'a> {
    fn new(map: &'a Map<String, Value>) -> Self {
        Index(map.iter().map(|(k, v)| (k.as_str(), v)).collect())
    }
    fn get(&self, key: &str) -> Option<&'a Value> {
        self.0.get(key).copied()
    }
    /// The entry with the index's own (message-lifetime) key.
    fn get_key_value(&self, key: &str) -> Option<(&'a str, &'a Value)> {
        self.0.get_key_value(key).map(|(k, v)| (*k, *v))
    }
}

pub struct Ranker<'a> {
    /// Result index to dump r_graph / admittances / Laplacian for
    /// (ARAGORN_SCORE_DEBUG_RESULT), and the index being scored.
    debug_result: Option<usize>,
    current: usize,
    kg_nodes: Index<'a>,
    kg_edges: Index<'a>,
    agraphs: Index<'a>,
    qgraph: &'a Map<String, Value>,
    default_weight: f64,
    probes: Option<R<Vec<(&'a str, &'a str)>>>,
    qedge_pairs: Option<R<Vec<(&'a str, &'a str)>>>,
    admittances: FxHashMap<&'a str, R<Rc<[f64]>>>,
    node_pubs: FxHashMap<&'a str, f64>,
    closures: FxHashMap<&'a str, ClosureRef<'a>>,
}

fn sub_map<'a>(v: Option<&'a Value>) -> &'a Map<String, Value> {
    v.and_then(Value::as_object).unwrap_or_else(|| empty_map())
}

impl<'a> Ranker<'a> {
    pub fn new(message: &'a Map<String, Value>) -> Self {
        let kgraph = sub_map(message.get("knowledge_graph"));
        Ranker {
            debug_result: std::env::var("ARAGORN_SCORE_DEBUG_RESULT")
                .ok()
                .and_then(|v| v.parse().ok()),
            current: 0,
            kg_nodes: Index::new(sub_map(kgraph.get("nodes"))),
            kg_edges: Index::new(sub_map(kgraph.get("edges"))),
            agraphs: Index::new(sub_map(message.get("auxiliary_graphs"))),
            qgraph: sub_map(message.get("query_graph")),
            default_weight: -1.0 / (1e-3f64).ln(),
            probes: None,
            qedge_pairs: None,
            admittances: FxHashMap::default(),
            node_pubs: FxHashMap::default(),
            closures: FxHashMap::default(),
        }
    }

    /// Score every answer, then return the indices in ranked order (the
    /// worker sorts ascending by each answer's best analysis score, stably).
    /// Also returns the reason each answer that failed to score failed, as
    /// (result index, message).
    pub fn rank(&mut self, answers: &mut [Value]) -> (Vec<usize>, Vec<(usize, String)>) {
        let mut failures = vec![];
        let mut keys = Vec::with_capacity(answers.len());
        for (index, answer) in answers.iter_mut().enumerate() {
            self.current = index;
            let outcome = self.score(&*answer);
            if let Outcome::Failed { error, .. } = &outcome {
                failures.push((index, error.clone()));
            }
            write_scores(answer, outcome);
            keys.push(sort_key(answer));
        }
        let mut order: Vec<usize> = (0..answers.len()).collect();
        order.sort_by(|&a, &b| keys[a].partial_cmp(&keys[b]).unwrap_or(std::cmp::Ordering::Equal));
        (order, failures)
    }

    /// Only reads `answer`; `rank` writes the scores once this returns.
    fn score(&mut self, answer: &Value) -> Outcome {
        let analyses_len = answer
            .get("analyses")
            .filter(|v| truthy(v))
            .and_then(Value::as_array)
            .map(|a| a.len())
            .unwrap_or(0);
        let fail = |scores: Vec<Value>, error: String| {
            let failed_at = scores.len();
            Outcome::Failed {
                scores,
                failed_at,
                error,
            }
        };
        let probes = match self.probes() {
            Ok(p) => p,
            Err(e) => return fail(vec![], e),
        };
        let r_graphs = match self.get_rgraph(answer) {
            Ok(g) => g,
            Err(e) => return fail(vec![], e),
        };
        let mut scores = Vec::with_capacity(analyses_len);
        for r_graph in &r_graphs {
            match self.score_rgraph(r_graph, &probes) {
                Ok(v) => scores.push(v),
                Err(e) => return fail(scores, e),
            }
        }
        Outcome::Scored(scores)
    }

    fn score_rgraph(&mut self, r_graph: &RGraph<'a, '_>, probes: &[(&'a str, &'a str)]) -> R<Value> {
        let (laplacian, probe_inds) = self.graph_laplacian(r_graph, probes)?;
        let n = laplacian.nrows();
        for j in 0..n {
            if (0..n).all(|i| laplacian[(i, j)] == 0.0) {
                return Ok(Value::from(0));
            }
        }
        let k = kirchhoff(&laplacian, &probe_inds)?;
        let score = (-k).exp();
        let score = if score.is_finite() && score >= 0.0 {
            score
        } else {
            -1.0
        };
        Ok(Value::from(score))
    }

    fn probes(&mut self) -> R<Vec<(&'a str, &'a str)>> {
        if self.probes.is_none() {
            self.probes = Some(self.find_probes());
        }
        self.probes.clone().unwrap()
    }

    fn find_probes(&self) -> R<Vec<(&'a str, &'a str)>> {
        let qnodes: Vec<&'a str> = sub_map(self.qgraph.get("nodes"))
            .keys()
            .map(String::as_str)
            .collect();
        let n = qnodes.len();
        if n == 0 {
            return Err("zero-size array to reduction operation maximum".into());
        }
        let index = |id: &str| {
            qnodes
                .iter()
                .position(|q| *q == id)
                .ok_or_else(|| format!("'{id}' is not in list"))
        };
        let mut conn = vec![vec![0u32; n]; n];
        for e in sub_map(self.qgraph.get("edges")).values() {
            let e = e.as_object().ok_or("a q_edge is not an object")?;
            let s = index(as_id(e.get("subject").ok_or("q_edge without subject")?)?)?;
            let o = index(as_id(e.get("object").ok_or("q_edge without object")?)?)?;
            conn[s][o] = 1;
        }
        let node_conn: Vec<u32> = (0..n)
            .map(|k| (0..n).map(|i| conn[i][k]).sum::<u32>() + conn[k].iter().sum::<u32>())
            .collect();
        let max = *node_conn.iter().max().unwrap();
        let mut probe_nodes = vec![];
        for c in 0..max {
            probe_nodes.extend((0..n).filter(|&k| node_conn[k] == c + 1));
            if probe_nodes.len() > 1 {
                break;
            }
        }
        let mut probes = vec![];
        for a in 0..probe_nodes.len() {
            for b in a + 1..probe_nodes.len() {
                probes.push((qnodes[probe_nodes[a]], qnodes[probe_nodes[b]]));
            }
        }
        Ok(probes)
    }

    fn qedge_pairs(&mut self) -> R<Vec<(&'a str, &'a str)>> {
        if self.qedge_pairs.is_none() {
            let pairs: R<Vec<_>> = sub_map(self.qgraph.get("edges"))
                .values()
                .map(|e| {
                    let e = e.as_object().ok_or("a q_edge is not an object")?;
                    Ok((
                        as_id(e.get("subject").ok_or("q_edge without subject")?)?,
                        as_id(e.get("object").ok_or("q_edge without object")?)?,
                    ))
                })
                .collect();
            self.qedge_pairs = Some(pairs);
        }
        self.qedge_pairs.clone().unwrap()
    }

    // --- r_graph ----------------------------------------------------------

    fn closure(&mut self, edge_id: &'a str) -> R<ClosureRef<'a>> {
        if let Some(c) = self.closures.get(edge_id) {
            return Ok(c.clone());
        }
        let c: ClosureRef<'a> = Rc::default();
        // Cached before walking: an edge reached again through a cycle adds
        // nothing new (as in the Python memo).
        self.closures.insert(edge_id, c.clone());
        let edge = match self.kg_edges.get(edge_id) {
            Some(e) if truthy(e) => e.as_object().ok_or("a k_edge is not an object")?,
            _ => return Ok(c),
        };
        {
            let mut cm = c.borrow_mut();
            cm.edges.insert(edge_id);
            for key in ["subject", "object"] {
                if let Some(v) = edge.get(key).filter(|v| truthy(v)) {
                    cm.nodes.insert(as_id(v)?);
                }
            }
        }
        for sg_id in edge_support_graphs(edge)? {
            let sg = match self.agraphs.get(sg_id) {
                Some(sg) if truthy(sg) => sg.as_object().ok_or("an aux graph is not an object")?,
                _ => continue,
            };
            for n in list_or_empty(sg, "nodes")? {
                c.borrow_mut().nodes.insert(as_id(n)?);
            }
            for add_edge in list_or_empty(sg, "edges")? {
                let add_edge_id = match self.kg_edges.get_key_value(as_id(add_edge)?) {
                    Some((k, _)) => k,
                    None => continue,
                };
                c.borrow_mut().edges.insert(add_edge_id);
                let child = self.closure(add_edge_id)?;
                if !Rc::ptr_eq(&child, &c) {
                    let ch = child.borrow();
                    let mut cm = c.borrow_mut();
                    cm.edges.extend(ch.edges.iter().copied());
                    cm.nodes.extend(ch.nodes.iter().copied());
                }
            }
        }
        Ok(c)
    }

    /// Ids from the answer live as long as the answer (`'b`); anything that
    /// is cached across answers is keyed by the knowledge graph's own strings.
    fn get_rgraph<'b>(&mut self, answer: &'b Value) -> R<Vec<RGraph<'a, 'b>>>
    where
        'a: 'b,
    {
        let answer = answer.as_object().ok_or("a result is not an object")?;
        let node_bindings = match answer.get("node_bindings") {
            Some(Value::Object(m)) => m,
            Some(v) if truthy(v) => return Err("node_bindings is not an object".into()),
            _ => empty_map(),
        };
        let qnodes: Vec<&'b str> = node_bindings.keys().map(String::as_str).collect();
        // k_node -> the r_nodes (q_node indices) it is bound to
        let mut bound_map: FxHashMap<&'b str, Vec<u32>> = FxHashMap::default();
        let mut bound_kids: IndexSet<&'b str> = IndexSet::default();
        for (qi, nb) in node_bindings.values().enumerate() {
            for kid in binding_ids(nb)? {
                bound_map.entry(kid).or_default().push(qi as u32);
                bound_kids.insert(kid);
            }
        }

        let mut out = vec![];
        for anal in list_or_falsy(answer, "analyses")? {
            let anal = anal.as_object().ok_or("an analysis is not an object")?;
            let mut node_ids = bound_kids.clone();
            let mut edge_ids: IndexSet<&'b str> = IndexSet::default();
            let edge_bindings = match anal.get("edge_bindings") {
                Some(Value::Object(m)) => m,
                Some(v) if truthy(v) => return Err("edge_bindings is not an object".into()),
                _ => empty_map(),
            };
            for eb in edge_bindings.values() {
                for e in binding_ids(eb)? {
                    edge_ids.insert(e);
                }
            }
            for sg_id in list_or_falsy(anal, "support_graphs")? {
                if let Some(sg) = self.agraphs.get(as_id(sg_id)?).filter(|v| truthy(v)) {
                    let sg = sg.as_object().ok_or("an aux graph is not an object")?;
                    for n in list_or_empty(sg, "nodes")? {
                        node_ids.insert(as_id(n)?);
                    }
                    for e in list_or_empty(sg, "edges")? {
                        edge_ids.insert(as_id(e)?);
                    }
                }
            }
            let current: Vec<&'b str> = edge_ids.iter().copied().collect();
            for e in current {
                // An id not in the KG has an empty closure.
                let e = match self.kg_edges.get_key_value(e) {
                    Some((k, _)) => k,
                    None => continue,
                };
                let c = self.closure(e)?;
                let c = c.borrow();
                edge_ids.extend(c.edges.iter().copied());
                node_ids.extend(c.nodes.iter().copied());
            }

            // r_nodes: the q_nodes, then one per support (non-bound) k_node.
            let mut support_index: FxHashMap<&'b str, u32> = FxHashMap::default();
            let mut n_nodes = qnodes.len() as u32;
            for sn in node_ids.iter().filter(|n| !bound_kids.contains(*n)) {
                support_index.insert(sn, n_nodes);
                n_nodes += 1;
            }
            let r_ids = |kid: &str| -> &[u32] {
                if let Some(v) = bound_map.get(kid) {
                    v
                } else if let Some(i) = support_index.get(kid) {
                    std::slice::from_ref(i)
                } else {
                    &[]
                }
            };
            // bound edges + support edges == every edge found
            let mut edges = IndexSet::default();
            for e_id in &edge_ids {
                let (e_id, e) = match self.kg_edges.get_key_value(*e_id) {
                    Some((k, e)) if truthy(e) => (
                        k,
                        e.as_object().ok_or("a k_edge is not an object")?,
                    ),
                    _ => continue, // "Edge not found in knowledge graph"
                };
                let s = as_id(e.get("subject").ok_or("KeyError: 'subject'")?)?;
                let o = as_id(e.get("object").ok_or("KeyError: 'object'")?)?;
                for &rs in r_ids(s) {
                    for &ro in r_ids(o) {
                        edges.insert((rs, ro, e_id));
                    }
                }
            }
            out.push(RGraph {
                qnodes: qnodes.clone(),
                n_nodes: n_nodes as usize,
                edges,
            });
        }
        Ok(out)
    }

    // --- edge values --------------------------------------------------------

    fn node_pubs(&mut self, node_id: &'a str) -> R<f64> {
        if let Some(v) = self.node_pubs.get(node_id) {
            return Ok(*v);
        }
        let node = match self.kg_nodes.get(node_id) {
            Some(n) if truthy(n) => n.as_object().ok_or("a k_node is not an object")?,
            _ => return Err(format!("Invalid node ID {node_id}")),
        };
        let mut count = Value::from(0);
        for p in list_or_empty(node, "attributes")? {
            let p = p.as_object().ok_or("an attribute is not an object")?;
            let name = p.get("original_attribute_name").and_then(Value::as_str);
            if matches!(name, Some("omnicorp_article_count") | Some("num_publications")) {
                count = p.get("value").ok_or("KeyError: 'value'")?.clone();
                break;
            }
        }
        let v = py_int_or_zero(&count)?;
        self.node_pubs.insert(node_id, v);
        Ok(v)
    }

    /// `get_edge_values` reduced to what the Laplacian uses: -1/ln(weight) of
    /// each weighted property, in the order Python sums them.
    fn admittances(&mut self, edge_id: &'a str) -> R<Rc<[f64]>> {
        if let Some(v) = self.admittances.get(edge_id) {
            return v.clone();
        }
        let v = self.compute_admittances(edge_id);
        self.admittances.insert(edge_id, v.clone());
        v
    }

    fn compute_admittances(&mut self, edge_id: &'a str) -> R<Rc<[f64]>> {
        let edge = match self.kg_edges.get(edge_id) {
            Some(e) if truthy(e) => e.as_object().ok_or("a k_edge is not an object")?,
            _ => return Err(format!("Invalid edge ID {edge_id}")),
        };

        let mut source: &str = "unspecified";
        for s in list_or_empty(edge, "sources")? {
            let s = s.as_object().ok_or("a source is not an object")?;
            if s.get("resource_role").and_then(Value::as_str) == Some("primary_knowledge_source") {
                source = match s.get("resource_id") {
                    None => "unspecified",
                    Some(Value::String(r)) => r,
                    Some(_) => "\u{0}non-string resource_id",
                };
                break;
            }
        }
        let is_literature =
            edge.get("predicate").and_then(Value::as_str) == Some(LITERATURE_PREDICATE);

        let mut num_publications = Value::from(0);
        let mut literature: Option<f64> = None;
        let mut p_value: Option<Value> = None;
        let mut affinity: Option<Value> = None;
        let zero = Value::from(0);

        for attribute in list_or_empty(edge, "attributes")? {
            let attribute = attribute.as_object().ok_or("an attribute is not an object")?;
            let orig = str_or_empty(attribute, "original_attribute_name")?;
            let type_id = str_or_empty(attribute, "attribute_type_id")?;

            if orig == "publications"
                || type_id == "biolink:supporting_document"
                || type_id == "biolink:publications"
            {
                let n = match attribute.get("value") {
                    None => 0,
                    Some(Value::String(s)) => split_count(s),
                    Some(Value::Array(a)) if a.len() == 1 => match &a[0] {
                        Value::String(s) => split_count(s),
                        other => {
                            return Err(format!("'in' on a {} publication", type_name(other)))
                        }
                    },
                    Some(Value::Array(a)) => a.len(),
                    Some(Value::Object(o)) if o.len() != 1 => o.len(),
                    Some(other) => {
                        return Err(format!("publications value is a {}", type_name(other)))
                    }
                };
                num_publications = Value::from(n);
            }

            if type_id == "biolink:evidence_count" {
                num_publications = attribute.get("value").cloned().unwrap_or(Value::from(0));
            }

            if orig.contains("p_value")
                || orig.contains("p-value")
                || type_id.contains("p_value")
                || type_id.contains("p-value")
                || orig.contains("pValue")
                || orig.contains("fisher_exact_p")
                || orig.contains("gwas_pvalue")
            {
                let mut v = attribute.get("value").cloned().unwrap_or(Value::Null);
                if let Value::Array(a) = &v {
                    v = a.first().cloned().unwrap_or(Value::Null);
                }
                if let Value::String(s) = &v {
                    v = s
                        .trim()
                        .parse::<f64>()
                        .ok()
                        .and_then(serde_json::Number::from_f64)
                        .map(Value::Number)
                        .unwrap_or(Value::Null);
                }
                p_value = if v.is_null() { None } else { Some(v) };
            }

            if is_literature && type_id == "biolink:has_count" {
                let num_pubs = py_int_or_zero(attribute.get("value").unwrap_or(&zero))?;
                let s = as_id(edge.get("subject").ok_or("KeyError: 'subject'")?)?;
                let o = as_id(edge.get("object").ok_or("KeyError: 'object'")?)?;
                let subject_pubs = match self.kg_nodes.get_key_value(s) {
                    Some((k, _)) => self.node_pubs(k)?,
                    None => return Err(format!("Invalid node ID {s}")),
                };
                let object_pubs = match self.kg_nodes.get_key_value(o) {
                    Some((k, _)) => self.node_pubs(k)?,
                    None => return Err(format!("Invalid node ID {o}")),
                };
                let cov = (num_pubs / TOTAL_PUBS)
                    - (subject_pubs / TOTAL_PUBS) * (object_pubs / TOTAL_PUBS);
                // Python's max((cov, 0.0)): cov unless 0.0 > cov.
                let cov = if 0.0 > cov { 0.0 } else { cov };
                literature = Some(cov * TOTAL_PUBS);
            }

            if orig == "affinity" {
                let v = attribute.get("value").cloned().unwrap_or(Value::from(0));
                affinity = if v.is_null() { None } else { Some(v) };
            }
        }

        let mut weights = vec![base_weight(source)];
        let mut push = |value: f64, property: &str| -> R<()> {
            let w = source_sigmoid(value, source, property)? * source_weight(source, property);
            weights.push(w);
            Ok(())
        };
        if let Some(v) = &p_value {
            push(as_num(v)?, "p_value")?;
        }
        if truthy(&num_publications) {
            push(as_num(&num_publications)?, "publications")?;
        }
        if let Some(v) = literature {
            push(v, "literature_co-occurrence")?;
        }
        if let Some(v) = &affinity {
            push(as_num(v)?, "affinity")?;
        }

        Ok(weights
            .into_iter()
            .map(|w| {
                let w = if w >= 1.0 { 0.9999999 } else { w };
                -1.0 / w.ln()
            })
            .collect())
    }

    // --- Laplacian ----------------------------------------------------------

    fn graph_laplacian(
        &mut self,
        r_graph: &RGraph<'a, '_>,
        probes: &[(&'a str, &'a str)],
    ) -> R<(DMatrix<f64>, Vec<(usize, usize)>)> {
        let n = r_graph.n_nodes;
        let q_index = |q: &str| r_graph.qnodes.iter().position(|x| *x == q);
        let mut w = DMatrix::<f64>::zeros(n, n);

        // q_node pairs joined by a q_edge start at the default weight.
        for (s, o) in self.qedge_pairs()? {
            if let (Some(i), Some(j)) = (q_index(s), q_index(o)) {
                w[(i, j)] = self.default_weight;
                w[(j, i)] = self.default_weight;
            }
        }
        // Each distinct k_edge between an (ordered) pair adds its admittances.
        let mut seen: FxHashSet<(u32, u32, &'a str)> = FxHashSet::default();
        for &(s, o, e) in &r_graph.edges {
            let adm = self.admittances(e)?;
            for (i, j) in [(s, o), (o, s)] {
                if seen.insert((i, j, e)) {
                    let cell = &mut w[(i as usize, j as usize)];
                    for a in adm.iter() {
                        *cell += a;
                    }
                }
            }
        }

        let mut lap = -w.clone();
        for i in 0..n {
            lap[(i, i)] = w.row(i).sum();
        }

        // Drop all-zero columns, but never a probe.
        let mut keep: Vec<bool> = (0..n)
            .map(|j| !(0..n).all(|i| lap[(i, j)].abs() == 0.0))
            .collect();
        let mut probe_rows = vec![];
        for (a, b) in probes {
            let ia = q_index(a).ok_or_else(|| format!("'{a}' is not in list"))?;
            let ib = q_index(b).ok_or_else(|| format!("'{b}' is not in list"))?;
            keep[ia] = true;
            keep[ib] = true;
            probe_rows.push((ia, ib));
        }
        let kept: Vec<usize> = (0..n).filter(|&i| keep[i]).collect();
        if self.debug_result == Some(self.current) {
            eprintln!("[debug result {}] q_nodes {:?}, {} r_nodes", self.current, r_graph.qnodes, n);
            for &(s, o, e) in &r_graph.edges {
                eprintln!("  r_edge ({s}, {o}) {e}: admittances {:?}", self.admittances.get(e));
            }
            eprintln!("  laplacian (before dropping zero columns) {lap}");
            eprintln!("  kept {kept:?}");
        }
        let pos = |i: usize| kept.iter().position(|&k| k == i).unwrap();
        let probe_inds = probe_rows.iter().map(|&(a, b)| (pos(a), pos(b))).collect();
        let m = kept.len();
        let reduced = DMatrix::from_fn(m, m, |i, j| lap[(kept[i], kept[j])]);
        Ok((reduced, probe_inds))
    }
}

fn split_count(s: &str) -> usize {
    if s.contains('|') {
        s.split('|').count()
    } else if s.contains(',') {
        s.split(',').count()
    } else {
        1
    }
}

/// Kirchhoff index restricted to the probes: trace(X^T L^+ X), with L^+ X
/// the least-squares solution numpy's `lstsq(L, X, rcond=None)` returns:
/// singular values at or below eps * max(n, n) * s_max count as zero.
///
/// The SVD is faer's. nalgebra's SVD and symmetric eigendecomposition were
/// both tried first and both, on some of these Laplacians, silently returned
/// unconverged decompositions (reconstruction error up to ~1, scores off by
/// up to 30%). As a guard, a decomposition that doesn't reconstruct L is an
/// error for that answer -- visible in compare.py -- rather than a wrong score.
fn kirchhoff(l: &DMatrix<f64>, probes: &[(usize, usize)]) -> R<f64> {
    if probes.is_empty() {
        return Ok(f64::NEG_INFINITY); // np.stack([]) raises -> -inf
    }
    let n = l.nrows();
    if l.iter().any(|v| !v.is_finite()) {
        return Err("SVD did not converge in Linear Least Squares".into());
    }
    let a = faer::Mat::<f64>::from_fn(n, n, |i, j| l[(i, j)]);
    let svd = a
        .svd()
        .map_err(|e| format!("SVD did not converge in Linear Least Squares ({e:?})"))?;
    let (u, s, v) = (svd.U(), svd.S().column_vector(), svd.V());

    let scale = l.iter().fold(0.0f64, |m, x| m.max(x.abs())).max(1.0);
    let mut err = 0.0f64;
    for i in 0..n {
        for j in 0..n {
            let mut r = 0.0;
            for k in 0..n {
                r += u[(i, k)] * s[k] * v[(j, k)];
            }
            err = err.max((r - l[(i, j)]).abs());
        }
    }
    if err > 1e-8 * scale {
        return Err(format!("SVD reconstruction error {err:e}"));
    }

    let smax = (0..n).fold(0.0f64, |m, k| m.max(s[k]));
    let cutoff = f64::EPSILON * n as f64 * smax; // rcond=None
    // For each probe column x = e_b - e_a: x^T V S+ U^T x.
    let mut total = 0.0;
    for &(pa, pb) in probes {
        let xc = |row: usize| -> f64 {
            if row == pb {
                1.0
            } else if row == pa {
                -1.0
            } else {
                0.0
            }
        };
        let mut k_c = 0.0;
        for k in 0..n {
            if s[k] > cutoff {
                let ut_x = xc(pa) * u[(pa, k)] + if pb != pa { xc(pb) * u[(pb, k)] } else { 0.0 };
                let vt_x = xc(pa) * v[(pa, k)] + if pb != pa { xc(pb) * v[(pb, k)] } else { 0.0 };
                k_c += vt_x * ut_x / s[k];
            }
        }
        total += k_c;
    }
    Ok(total)
}

fn write_scores(answer: &mut Value, outcome: Outcome) {
    let analyses = match answer.get_mut("analyses").and_then(Value::as_array_mut) {
        Some(a) => a,
        None => return,
    };
    let (scores, failed_at) = match outcome {
        Outcome::Scored(s) => {
            let n = s.len();
            (s, n)
        }
        Outcome::Failed {
            scores, failed_at, ..
        } => (scores, failed_at),
    };
    for (i, analysis) in analyses.iter_mut().enumerate() {
        if let Some(a) = analysis.as_object_mut() {
            if i < failed_at {
                a.insert("score".into(), scores[i].clone());
            } else if !a.contains_key("score") {
                a.insert("score".into(), Value::from(0));
            }
        }
    }
}

/// `max(analysis["score"] for analysis in answer.get("analyses") or [{"score": 0}])`
fn sort_key(answer: &Value) -> f64 {
    let analyses = answer
        .get("analyses")
        .filter(|v| truthy(v))
        .and_then(Value::as_array);
    let mut best: Option<f64> = None;
    for a in analyses.map(|a| a.as_slice()).unwrap_or(&[]) {
        let s = a.get("score").and_then(Value::as_f64).unwrap_or(0.0);
        // Python's max keeps the first of equal values; only strictly greater
        // replaces it.
        if best.map_or(true, |b| s > b) {
            best = Some(s);
        }
    }
    best.unwrap_or(0.0)
}
