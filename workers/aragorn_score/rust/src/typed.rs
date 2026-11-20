//! The typed ranker (`--parse typed`): the same algorithm as
//! `value_ranker.rs`, over the borrowed structs of `typed_input.rs`.
//!
//! The message is turned into a compact model first: every node, edge and
//! auxiliary-graph id interned to an integer, every edge's admittances
//! computed once. Python's per-item errors (an edge whose node is missing
//! from the KG, a null where a list belongs) are stored in the model and
//! raised where the Python code would raise them, so a failing answer fails
//! the same way. Scoring then works on integers instead of strings.
//!
//! The output is written by splicing: the input bytes are copied, with the
//! results array rewritten in ranked order and `"score"` inserted into each
//! analysis, instead of serialising the whole message again.

use std::cell::RefCell;
use std::rc::Rc;

use indexmap::IndexSet as StdIndexSet;
use nalgebra::DMatrix;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde_json::Value;

use crate::solve::{kirchhoff, SolveStats, Solver};
use crate::typed_input::{
    name_str, AnalysisFields, AuxFields, EdgeFields, Has, Maybe, NodeFields, Parsed,
    ParsedResult, Thing, AV, R,
};
use crate::value_ranker::{
    base_weight, source_sigmoid, source_weight, split_count, LITERATURE_PREDICATE,
    SUPPORT_GRAPHS_ATTRIBUTE, TOTAL_PUBS,
};

type IndexSet<T> = StdIndexSet<T, FxBuildHasher>;
type NodeId = u32;
type EdgeId = u32;
type AuxId = u32;

// --- the model -------------------------------------------------------------------

/// An edge's subject or object, as the r_graph code sees it.
enum Endpoint {
    Missing,
    Falsy,
    Node(NodeId),
    Bad(&'static str),
}

enum EdgeEntry {
    /// `if not edge` (null, {}, ...)
    Falsy,
    /// A truthy non-object.
    Bad(&'static str),
    Data(EdgeData),
}

struct EdgeData {
    subject: Endpoint,
    object: Endpoint,
    /// biolink:support_graphs ids (None: not an auxiliary graph).
    support: R<Vec<Option<AuxId>>>,
    /// -1/ln(weight) per weighted property, or the error get_edge_values raises.
    admittances: R<Rc<[f64]>>,
}

enum AuxEntry {
    Falsy,
    Bad(&'static str),
    Data {
        nodes: R<Vec<NodeId>>,
        edges: R<Vec<Option<EdgeId>>>,
    },
}

struct AnswerData<'m> {
    qnodes: Vec<&'m str>,
    /// (q_node index, k_node) for every node binding id, in order.
    bound: Vec<(u32, NodeId)>,
    analyses: Vec<AnalysisData>,
}

struct AnalysisData {
    edges: Vec<EdgeId>,
    aux: Vec<AuxId>,
}

struct AnalysisMeta {
    is_object: bool,
    /// The analysis already had a score (kept by Python's setdefault when the
    /// answer fails), and its numeric value.
    existing: Option<f64>,
}

struct AnswerEntry<'m> {
    data: R<AnswerData<'m>>,
    meta: Vec<AnalysisMeta>,
}

pub struct Model<'m> {
    probes: R<Vec<(&'m str, &'m str)>>,
    qedge_pairs: R<Vec<(&'m str, &'m str)>>,
    edges: Vec<EdgeEntry>,
    edge_names: Vec<&'m str>,
    aux: Vec<AuxEntry>,
    answers: Vec<AnswerEntry<'m>>,
}

#[derive(Default)]
struct Interner<'m> {
    ix: FxHashMap<&'m str, u32>,
    names: Vec<&'m str>,
}

impl<'m> Interner<'m> {
    fn intern(&mut self, s: &'m str) -> u32 {
        if let Some(&i) = self.ix.get(s) {
            return i;
        }
        let i = self.names.len() as u32;
        self.ix.insert(s, i);
        self.names.push(s);
        i
    }
}

fn list<'x, T>(m: &'x Maybe<Vec<T>>, what: &str) -> R<&'x [T]> {
    match m {
        Maybe::Missing => Ok(&[]),
        Maybe::Null => Err(format!("'NoneType' object is not iterable ({what})")),
        Maybe::Present(v) => Ok(v),
    }
}

/// `x.get(key) or []` as a list.
fn list_or_falsy<T>(m: &Maybe<Vec<T>>) -> &[T] {
    match m {
        Maybe::Present(v) => v,
        _ => &[],
    }
}

fn find_probes<'m>(qnodes: &[&'m str], qedges: &[(&'m str, &'m str)]) -> R<Vec<(&'m str, &'m str)>> {
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
    for (s, o) in qedges {
        conn[index(s)?][index(o)?] = 1;
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

fn node_pubs(id: &str, node: &Thing<NodeFields>) -> R<f64> {
    let f = match node {
        t if !t.truthy() => return Err(format!("Invalid node ID {id}")),
        Thing::Obj { f, .. } => f,
        other => return Err(format!("'{}' object has no attribute 'get'", other.kind())),
    };
    let mut count = AV::Int(0);
    for p in list(&f.attributes, "node attributes")? {
        let p = p
            .obj()
            .ok_or_else(|| format!("'{}' object has no attribute 'get'", p.kind()))?;
        if matches!(&p.orig.0, Some(AV::Str(s)) if s == "omnicorp_article_count" || s == "num_publications")
        {
            count = p.value.0.av().ok_or("KeyError: 'value'")?;
            break;
        }
    }
    count.py_int_or_zero()
}

/// `get_edge_values`, reduced to the admittance of each weighted property.
fn edge_admittances(f: &EdgeFields, pubs: &dyn Fn(&AV) -> R<f64>) -> R<Rc<[f64]>> {
    let mut source: &str = "unspecified";
    for s in list(&f.sources, "sources")? {
        let s = s
            .obj()
            .ok_or_else(|| format!("'{}' object has no attribute 'get'", s.kind()))?;
        if matches!(&s.role.0, Some(AV::Str(r)) if r == "primary_knowledge_source") {
            source = match &s.id.0 {
                None => "unspecified",
                Some(AV::Str(r)) => r,
                Some(_) => "\u{0}non-string resource_id",
            };
            break;
        }
    }
    let is_literature = matches!(&f.predicate.0, Some(AV::Str(p)) if p == LITERATURE_PREDICATE);

    let mut num_publications = AV::Int(0);
    let mut literature: Option<f64> = None;
    let mut p_value: Option<AV> = None;
    let mut affinity: Option<AV> = None;

    for a in list(&f.attributes, "attributes")? {
        let a = a
            .obj()
            .ok_or_else(|| format!("'{}' object has no attribute 'get'", a.kind()))?;
        let orig = name_str(&a.orig)?;
        let type_id = name_str(&a.type_id)?;
        let value = &a.value.0;

        if orig == "publications"
            || type_id == "biolink:supporting_document"
            || type_id == "biolink:publications"
        {
            let n = match value.av() {
                None => 0,
                Some(AV::Str(s)) => split_count(&s),
                Some(AV::List { len: 1, first }) => match first.as_deref() {
                    Some(AV::Str(s)) => split_count(s),
                    Some(o) => return Err(format!("'in' on a {} publication", o.type_name())),
                    None => 0,
                },
                Some(AV::List { len, .. }) => len,
                Some(AV::Obj { len }) if len != 1 => len,
                Some(o) => return Err(format!("publications value is a {}", o.type_name())),
            };
            num_publications = AV::UInt(n as u64);
        }

        if type_id == "biolink:evidence_count" {
            num_publications = value.av().unwrap_or(AV::Int(0));
        }

        if orig.contains("p_value")
            || orig.contains("p-value")
            || type_id.contains("p_value")
            || type_id.contains("p-value")
            || orig.contains("pValue")
            || orig.contains("fisher_exact_p")
            || orig.contains("gwas_pvalue")
        {
            let mut v = value.av().unwrap_or(AV::Null);
            if let AV::List { first, .. } = v {
                v = first.map(|b| *b).unwrap_or(AV::Null);
            }
            if let AV::Str(s) = &v {
                v = s.trim().parse::<f64>().map(AV::Float).unwrap_or(AV::Null);
            }
            p_value = if matches!(v, AV::Null) { None } else { Some(v) };
        }

        if is_literature && type_id == "biolink:has_count" {
            let num_pubs = value.av().unwrap_or(AV::Int(0)).py_int_or_zero()?;
            let s = f.subject.0.as_ref().ok_or("KeyError: 'subject'")?;
            let o = f.object.0.as_ref().ok_or("KeyError: 'object'")?;
            let subject_pubs = pubs(s)?;
            let object_pubs = pubs(o)?;
            let cov =
                (num_pubs / TOTAL_PUBS) - (subject_pubs / TOTAL_PUBS) * (object_pubs / TOTAL_PUBS);
            let cov = if 0.0 > cov { 0.0 } else { cov };
            literature = Some(cov * TOTAL_PUBS);
        }

        if orig == "affinity" {
            let v = value.av().unwrap_or(AV::Int(0));
            affinity = if matches!(v, AV::Null) { None } else { Some(v) };
        }
    }

    let mut weights = vec![base_weight(source)];
    let mut push = |value: f64, property: &str| -> R<()> {
        weights.push(source_sigmoid(value, source, property)? * source_weight(source, property));
        Ok(())
    };
    if let Some(v) = &p_value {
        push(v.as_num()?, "p_value")?;
    }
    if num_publications.truthy() {
        push(num_publications.as_num()?, "publications")?;
    }
    if let Some(v) = literature {
        push(v, "literature_co-occurrence")?;
    }
    if let Some(v) = &affinity {
        push(v.as_num()?, "affinity")?;
    }
    Ok(weights
        .into_iter()
        .map(|w| {
            let w = if w >= 1.0 { 0.9999999 } else { w };
            -1.0 / w.ln()
        })
        .collect())
}

impl<'m> Model<'m> {
    pub fn build(p: &'m Parsed<'_>) -> Model<'m> {
        let msg = &p.message;

        // Query graph: probes and the q_node pairs joined by a q_edge.
        let (probes, qedge_pairs) = match &msg.query_graph.0 {
            None => (find_probes(&[], &[]), Ok(vec![])),
            Some(Thing::Obj { f, .. }) => {
                let qnodes: R<Vec<&str>> = match &f.nodes {
                    Maybe::Present(e) => {
                        let mut seen = FxHashSet::default();
                        Ok(e.0.iter().map(|(k, _)| &**k).filter(|k| seen.insert(*k)).collect())
                    }
                    Maybe::Missing => Err("KeyError: 'nodes'".into()),
                    Maybe::Null => Err("'NoneType' object has no attribute 'keys'".into()),
                };
                let qedges: R<Vec<(&str, &str)>> = match &f.edges {
                    Maybe::Present(e) => e
                        .0
                        .iter()
                        .map(|(_, qe)| {
                            let qe = qe.obj().ok_or("a q_edge is not an object")?;
                            let end = |v: &'m Has<AV>, key: &str| -> R<&'m str> {
                                match &v.0 {
                                    Some(AV::Str(s)) => Ok(s),
                                    Some(o) => Err(format!("q_edge {key} is a {}", o.type_name())),
                                    None => Err(format!("KeyError: '{key}'")),
                                }
                            };
                            Ok((end(&qe.subject, "subject")?, end(&qe.object, "object")?))
                        })
                        .collect(),
                    Maybe::Missing => Err("KeyError: 'edges'".into()),
                    Maybe::Null => Err("'NoneType' object has no attribute 'values'".into()),
                };
                let probes = match (&qnodes, &qedges) {
                    (Ok(n), Ok(e)) => find_probes(n, e),
                    (Err(e), _) | (_, Err(e)) => Err(e.clone()),
                };
                (probes, qedges)
            }
            Some(_) => {
                let e: R<Vec<_>> = Err("'NoneType' object is not subscriptable".into());
                (e.clone(), e)
            }
        };

        // Knowledge graph and auxiliary graphs (the last of duplicate keys wins).
        let (kg_nodes, kg_edges) = match &msg.knowledge_graph.0 {
            Some(Thing::Obj { f, .. }) => (
                match &f.nodes {
                    Maybe::Present(e) => e.0.as_slice(),
                    _ => &[],
                },
                match &f.edges {
                    Maybe::Present(e) => e.0.as_slice(),
                    _ => &[],
                },
            ),
            _ => (&[][..], &[][..]),
        };
        let aux_list = match &msg.auxiliary_graphs {
            Maybe::Present(e) => e.0.as_slice(),
            _ => &[],
        };

        let mut nodes = Interner::default();
        let mut edge_ix: FxHashMap<&str, EdgeId> = FxHashMap::default();
        let mut edge_src: Vec<(&str, &Thing<EdgeFields>)> = vec![];
        for (k, e) in kg_edges {
            match edge_ix.get(&**k) {
                Some(&i) => edge_src[i as usize] = (k, e),
                None => {
                    edge_ix.insert(k, edge_src.len() as u32);
                    edge_src.push((k, e));
                }
            }
        }
        let mut aux_ix: FxHashMap<&str, AuxId> = FxHashMap::default();
        let mut aux_src: Vec<&Thing<AuxFields>> = vec![];
        for (k, a) in aux_list {
            match aux_ix.get(&**k) {
                Some(&i) => aux_src[i as usize] = a,
                None => {
                    aux_ix.insert(k, aux_src.len() as u32);
                    aux_src.push(a);
                }
            }
        }
        let mut pubs: FxHashMap<&str, R<f64>> = FxHashMap::default();
        for (k, n) in kg_nodes {
            pubs.insert(k, node_pubs(k, n));
        }
        let pubs_of = |v: &AV| -> R<f64> {
            match v {
                AV::Str(s) => pubs
                    .get(&**s)
                    .cloned()
                    .unwrap_or_else(|| Err(format!("Invalid node ID {s}"))),
                other => Err(format!("Invalid node ID ({})", other.type_name())),
            }
        };

        // Edges.
        let mut edges = Vec::with_capacity(edge_src.len());
        let mut edge_names = Vec::with_capacity(edge_src.len());
        for (name, e) in &edge_src {
            edge_names.push(*name);
            let entry = match e {
                t if !t.truthy() => EdgeEntry::Falsy,
                Thing::Obj { f, .. } => {
                    let mut endpoint = |v: &'m Has<AV<'m>>| match &v.0 {
                        None => Endpoint::Missing,
                        Some(a) if !a.truthy() => Endpoint::Falsy,
                        Some(AV::Str(s)) => Endpoint::Node(nodes.intern(s)),
                        Some(a) => Endpoint::Bad(a.type_name()),
                    };
                    let subject = endpoint(&f.subject);
                    let object = endpoint(&f.object);
                    let support = edge_support(f, &aux_ix);
                    EdgeEntry::Data(EdgeData {
                        subject,
                        object,
                        support,
                        admittances: edge_admittances(f, &pubs_of),
                    })
                }
                other => EdgeEntry::Bad(other.kind()),
            };
            edges.push(entry);
        }

        // Auxiliary graphs.
        let aux = aux_src
            .iter()
            .map(|a| match a {
                t if !t.truthy() => AuxEntry::Falsy,
                Thing::Obj { f, .. } => AuxEntry::Data {
                    nodes: list(&f.nodes, "aux nodes").and_then(|l| {
                        l.iter()
                            .map(|n| match n {
                                AV::Str(s) => Ok(nodes.intern(s)),
                                o => Err(format!("aux graph node is a {}", o.type_name())),
                            })
                            .collect()
                    }),
                    edges: list(&f.edges, "aux edges").and_then(|l| {
                        l.iter()
                            .map(|e| match e {
                                AV::Str(s) => Ok(edge_ix.get(&**s).copied()),
                                o => Err(format!("aux graph edge is a {}", o.type_name())),
                            })
                            .collect()
                    }),
                },
                other => AuxEntry::Bad(other.kind()),
            })
            .collect();

        // Answers.
        let answers = p
            .results
            .iter()
            .flatten()
            .map(|r| answer_entry(r, &mut nodes, &edge_ix, &aux_ix))
            .collect();

        Model {
            probes,
            qedge_pairs,
            edges,
            edge_names,
            aux,
            answers,
        }
    }

    pub fn n_answers(&self) -> usize {
        self.answers.len()
    }
}

/// `edge_support_graphs`: tolerant of absent / null attributes.
fn edge_support(f: &EdgeFields, aux_ix: &FxHashMap<&str, AuxId>) -> R<Vec<Option<AuxId>>> {
    let mut out = vec![];
    for a in list_or_falsy(&f.attributes) {
        let a = a
            .obj()
            .ok_or_else(|| format!("'{}' object has no attribute 'get'", a.kind()))?;
        if !matches!(&a.type_id.0, Some(AV::Str(t)) if t == SUPPORT_GRAPHS_ATTRIBUTE) {
            continue;
        }
        match a.value.0.full() {
            Some(Value::Array(ids)) => {
                for id in ids {
                    match id {
                        Value::String(s) => out.push(aux_ix.get(s.as_str()).copied()),
                        other => return Err(format!("support graph id is {other}")),
                    }
                }
            }
            Some(Value::String(s)) => {
                // list.extend(str) extends by characters
                for c in s.chars() {
                    out.push(aux_ix.get(c.to_string().as_str()).copied());
                }
            }
            Some(v) if crate::value_ranker::truthy(&v) => {
                return Err("support_graphs value is not a list".into())
            }
            _ => {}
        }
    }
    Ok(out)
}

fn answer_entry<'m>(
    r: &'m ParsedResult<'_>,
    nodes: &mut Interner<'m>,
    edge_ix: &FxHashMap<&str, EdgeId>,
    aux_ix: &FxHashMap<&str, AuxId>,
) -> AnswerEntry<'m> {
    let meta = r
        .analyses
        .iter()
        .map(|(_, a)| AnalysisMeta {
            is_object: a.obj().is_some(),
            existing: a
                .obj()
                .and_then(|f: &AnalysisFields| f.score.0.as_ref())
                .map(|s| s.as_num().unwrap_or(0.0)),
        })
        .collect();

    let data = (|| -> R<AnswerData<'m>> {
        let f = r.fields.obj().ok_or("a result is not an object")?;
        let mut qnodes = vec![];
        let mut bound = vec![];
        if let Maybe::Present(nb) = &f.node_bindings {
            for (qi, (q, b)) in nb.0.iter().enumerate() {
                qnodes.push(&**q);
                if let Some(bf) = b.obj() {
                    for id in list_or_falsy(&bf.ids) {
                        match id {
                            AV::Str(s) => bound.push((qi as u32, nodes.intern(s))),
                            o => return Err(format!("a node binding id is a {}", o.type_name())),
                        }
                    }
                }
            }
        }
        let mut analyses = vec![];
        for (_, a) in &r.analyses {
            let a = a
                .obj()
                .ok_or_else(|| format!("'{}' object has no attribute 'get'", a.kind()))?;
            let mut edges = vec![];
            if let Maybe::Present(eb) = &a.edge_bindings {
                for (_, b) in &eb.0 {
                    if let Some(bf) = b.obj() {
                        for id in list_or_falsy(&bf.ids) {
                            match id {
                                AV::Str(s) => edges.extend(edge_ix.get(&**s).copied()),
                                o => return Err(format!("an edge binding id is a {}", o.type_name())),
                            }
                        }
                    }
                }
            }
            let mut aux = vec![];
            for id in list_or_falsy(&a.support_graphs) {
                match id {
                    AV::Str(s) => aux.extend(aux_ix.get(&**s).copied()),
                    o => return Err(format!("a support graph id is a {}", o.type_name())),
                }
            }
            analyses.push(AnalysisData { edges, aux });
        }
        Ok(AnswerData {
            qnodes,
            bound,
            analyses,
        })
    })();
    AnswerEntry { data, meta }
}

// --- scoring ------------------------------------------------------------------------

#[derive(Default)]
struct Closure {
    nodes: IndexSet<NodeId>,
    edges: IndexSet<EdgeId>,
}

type ClosureRef = Rc<RefCell<Closure>>;

/// The state of one support-closure walk (Tarjan's SCC algorithm).
#[derive(Default)]
struct Walk {
    index: FxHashMap<EdgeId, u32>,
    low: FxHashMap<EdgeId, u32>,
    stack: Vec<EdgeId>,
    on_stack: FxHashSet<EdgeId>,
    found: FxHashMap<EdgeId, ClosureRef>,
}

struct RGraph<'x> {
    qnodes: &'x [&'x str],
    n_nodes: usize,
    edges: IndexSet<(u32, u32, EdgeId)>,
}

/// What an analysis ends up with.
#[derive(Clone, Copy)]
pub enum Final {
    /// A new score: Some(x), or None for the int 0 Python writes.
    New(Option<f64>),
    /// Kept its existing score (Python's setdefault on a failed answer).
    Keep(f64),
    /// Not an object; left alone.
    Skip,
}

impl Final {
    fn numeric(self) -> f64 {
        match self {
            Final::New(Some(x)) => x,
            Final::New(None) | Final::Skip => 0.0,
            Final::Keep(x) => x,
        }
    }
    pub fn to_value(self) -> Value {
        match self {
            Final::New(Some(x)) => Value::from(x),
            Final::New(None) => Value::from(0),
            Final::Keep(x) => Value::from(x),
            Final::Skip => Value::Null,
        }
    }
}

pub struct Ranked {
    pub finals: Vec<Vec<Final>>,
    pub order: Vec<usize>,
    pub failures: Vec<(usize, String)>,
    pub stats: SolveStats,
}

pub struct Scorer<'x, 'm> {
    m: &'x Model<'m>,
    solver: Solver,
    stats: SolveStats,
    closures: Vec<Option<ClosureRef>>,
    default_weight: f64,
    debug_result: Option<usize>,
    current: usize,
}

impl<'x, 'm> Scorer<'x, 'm> {
    pub fn new(m: &'x Model<'m>, solver: Solver) -> Self {
        Scorer {
            m,
            solver,
            stats: SolveStats::default(),
            closures: vec![None; m.edges.len()],
            default_weight: -1.0 / (1e-3f64).ln(),
            debug_result: std::env::var("ARAGORN_SCORE_DEBUG_RESULT")
                .ok()
                .and_then(|v| v.parse().ok()),
            current: 0,
        }
    }

    pub fn rank(mut self) -> Ranked {
        let m = self.m;
        let mut finals = Vec::with_capacity(m.answers.len());
        let mut failures = vec![];
        let mut keys = Vec::with_capacity(m.answers.len());
        for (index, answer) in m.answers.iter().enumerate() {
            self.current = index;
            let (scores, error) = self.score(answer);
            let failed_at = scores.len();
            let fin: Vec<Final> = answer
                .meta
                .iter()
                .enumerate()
                .map(|(i, meta)| {
                    if !meta.is_object {
                        Final::Skip
                    } else if i < failed_at {
                        Final::New(scores[i])
                    } else {
                        // setdefault("score", 0) after a failure
                        meta.existing.map(Final::Keep).unwrap_or(Final::New(None))
                    }
                })
                .collect();
            if let Some(e) = error {
                failures.push((index, e));
            }
            // max(analysis["score"] for analysis in analyses or [{"score": 0}])
            let mut best: Option<f64> = None;
            for f in &fin {
                let s = f.numeric();
                if best.map_or(true, |b| s > b) {
                    best = Some(s);
                }
            }
            keys.push(best.unwrap_or(0.0));
            finals.push(fin);
        }
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by(|&a, &b| keys[a].partial_cmp(&keys[b]).unwrap_or(std::cmp::Ordering::Equal));
        Ranked {
            finals,
            order,
            failures,
            stats: self.stats,
        }
    }

    /// The scores of the analyses scored before any error, and the error.
    fn score(&mut self, answer: &'x AnswerEntry<'m>) -> (Vec<Option<f64>>, Option<String>) {
        let probes = match &self.m.probes {
            Ok(p) => p,
            Err(e) => return (vec![], Some(e.clone())),
        };
        let r_graphs = match self.get_rgraph(answer) {
            Ok(g) => g,
            Err(e) => return (vec![], Some(e)),
        };
        let mut scores = Vec::with_capacity(r_graphs.len());
        for r_graph in &r_graphs {
            match self.score_rgraph(r_graph, probes) {
                Ok(v) => scores.push(v),
                Err(e) => return (scores, Some(e)),
            }
        }
        (scores, None)
    }

    fn score_rgraph(&mut self, r_graph: &RGraph, probes: &[(&str, &str)]) -> R<Option<f64>> {
        let (laplacian, probe_inds) = self.graph_laplacian(r_graph, probes)?;
        let n = laplacian.nrows();
        for j in 0..n {
            if (0..n).all(|i| laplacian[(i, j)] == 0.0) {
                return Ok(None);
            }
        }
        let k = kirchhoff(&laplacian, &probe_inds, self.solver, &mut self.stats)?;
        let score = (-k).exp();
        Ok(Some(if score.is_finite() && score >= 0.0 { score } else { -1.0 }))
    }

    /// Every node and edge reachable from `e` through support graphs
    /// (cached per message). Support graphs can form cycles, and every edge
    /// in a cycle has the same closure: Tarjan's strongly-connected-components
    /// walk finds them, and a cycle's members get the closure of its first
    /// edge once that one's walk completes. (Caching each edge when its own
    /// walk ended left cycle members with partial, order-dependent closures.)
    /// The same algorithm as `Ranker._support_closure` in worker.py.
    fn closure(&mut self, e: EdgeId) -> R<ClosureRef> {
        if let Some(c) = &self.closures[e as usize] {
            return Ok(c.clone());
        }
        self.closure_walk(e, &mut Walk::default())
    }

    fn closure_walk(&mut self, e: EdgeId, w: &mut Walk) -> R<ClosureRef> {
        if let Some(c) = &self.closures[e as usize] {
            return Ok(c.clone());
        }
        let my_index = w.index.len() as u32;
        w.index.insert(e, my_index);
        w.low.insert(e, my_index);
        w.stack.push(e);
        w.on_stack.insert(e);
        let c: ClosureRef = Rc::default();
        w.found.insert(e, c.clone());

        match &self.m.edges[e as usize] {
            EdgeEntry::Falsy => {}
            EdgeEntry::Bad(t) => return Err(format!("'{t}' object has no attribute 'get'")),
            EdgeEntry::Data(d) => {
                {
                    let mut cm = c.borrow_mut();
                    cm.edges.insert(e);
                    for ep in [&d.subject, &d.object] {
                        match ep {
                            Endpoint::Node(n) => {
                                cm.nodes.insert(*n);
                            }
                            Endpoint::Bad(t) => return Err(format!("edge endpoint is a {t}")),
                            _ => {}
                        }
                    }
                }
                for aux in d.support.as_ref().map_err(Clone::clone)? {
                    let Some(a) = aux else { continue };
                    let (nodes, edges) = match &self.m.aux[*a as usize] {
                        AuxEntry::Falsy => continue,
                        AuxEntry::Bad(t) => {
                            return Err(format!("'{t}' object has no attribute 'get'"))
                        }
                        AuxEntry::Data { nodes, edges } => (nodes, edges),
                    };
                    c.borrow_mut()
                        .nodes
                        .extend(nodes.as_ref().map_err(Clone::clone)?.iter().copied());
                    for ae in edges.as_ref().map_err(Clone::clone)? {
                        let Some(ae) = *ae else { continue };
                        c.borrow_mut().edges.insert(ae);
                        let child = if w.on_stack.contains(&ae) {
                            // Back to an edge of a cycle still being walked:
                            // what it has found so far.
                            let i = w.index[&ae];
                            let low = w.low.get_mut(&e).unwrap();
                            *low = (*low).min(i);
                            w.found[&ae].clone()
                        } else {
                            // Not reached yet, or cached from an earlier walk.
                            let child = self.closure_walk(ae, w)?;
                            if w.on_stack.contains(&ae) {
                                let child_low = w.low[&ae];
                                let low = w.low.get_mut(&e).unwrap();
                                *low = (*low).min(child_low);
                            }
                            child
                        };
                        if !Rc::ptr_eq(&child, &c) {
                            let ch = child.borrow();
                            let mut cm = c.borrow_mut();
                            cm.edges.extend(ch.edges.iter().copied());
                            cm.nodes.extend(ch.nodes.iter().copied());
                        }
                    }
                }
            }
        }

        if w.low[&e] == my_index {
            // This edge closes its cycle (or is in none): its closure is
            // complete, and is every member's.
            loop {
                let member = w.stack.pop().expect("Tarjan stack");
                w.on_stack.remove(&member);
                self.closures[member as usize] = Some(c.clone());
                if member == e {
                    break;
                }
            }
        }
        Ok(c)
    }

    fn get_rgraph(&mut self, answer: &'x AnswerEntry<'m>) -> R<Vec<RGraph<'x>>> {
        let data = answer.data.as_ref().map_err(Clone::clone)?;
        let mut bound_map: FxHashMap<NodeId, Vec<u32>> = FxHashMap::default();
        let mut bound_set: IndexSet<NodeId> = IndexSet::default();
        for &(qi, n) in &data.bound {
            bound_map.entry(n).or_default().push(qi);
            bound_set.insert(n);
        }
        let mut out = Vec::with_capacity(data.analyses.len());
        for an in &data.analyses {
            let mut node_ids = bound_set.clone();
            let mut edge_ids: IndexSet<EdgeId> = an.edges.iter().copied().collect();
            for &a in &an.aux {
                match &self.m.aux[a as usize] {
                    AuxEntry::Falsy => {}
                    AuxEntry::Bad(t) => return Err(format!("'{t}' object has no attribute 'get'")),
                    AuxEntry::Data { nodes, edges } => {
                        node_ids.extend(nodes.as_ref().map_err(Clone::clone)?.iter().copied());
                        edge_ids.extend(edges.as_ref().map_err(Clone::clone)?.iter().flatten().copied());
                    }
                }
            }
            let current: Vec<EdgeId> = edge_ids.iter().copied().collect();
            for e in current {
                let c = self.closure(e)?;
                let c = c.borrow();
                edge_ids.extend(c.edges.iter().copied());
                node_ids.extend(c.nodes.iter().copied());
            }

            let mut support_index: FxHashMap<NodeId, u32> = FxHashMap::default();
            let mut n_nodes = data.qnodes.len() as u32;
            for &sn in node_ids.iter().filter(|n| !bound_set.contains(*n)) {
                support_index.insert(sn, n_nodes);
                n_nodes += 1;
            }
            let r_ids = |ep: &Endpoint, key: &str| -> R<&[u32]> {
                match ep {
                    Endpoint::Missing => Err(format!("KeyError: '{key}'")),
                    Endpoint::Falsy => Ok(&[]),
                    Endpoint::Bad(t) => Err(format!("edge {key} is a {t}")),
                    Endpoint::Node(n) => Ok(if let Some(v) = bound_map.get(n) {
                        v
                    } else if let Some(i) = support_index.get(n) {
                        std::slice::from_ref(i)
                    } else {
                        &[]
                    }),
                }
            };
            let mut edges = IndexSet::default();
            for &e in &edge_ids {
                let d = match &self.m.edges[e as usize] {
                    EdgeEntry::Falsy => continue, // "Edge not found in knowledge graph"
                    EdgeEntry::Bad(t) => return Err(format!("'{t}' object is not subscriptable")),
                    EdgeEntry::Data(d) => d,
                };
                let (s, o) = (r_ids(&d.subject, "subject")?, r_ids(&d.object, "object")?);
                for &rs in s {
                    for &ro in o {
                        edges.insert((rs, ro, e));
                    }
                }
            }
            out.push(RGraph {
                qnodes: &data.qnodes,
                n_nodes: n_nodes as usize,
                edges,
            });
        }
        Ok(out)
    }

    fn graph_laplacian(
        &mut self,
        r_graph: &RGraph,
        probes: &[(&str, &str)],
    ) -> R<(DMatrix<f64>, Vec<(usize, usize)>)> {
        let n = r_graph.n_nodes;
        let q_index = |q: &str| r_graph.qnodes.iter().position(|x| *x == q);
        let mut w = DMatrix::<f64>::zeros(n, n);
        for (s, o) in self.m.qedge_pairs.as_ref().map_err(Clone::clone)? {
            if let (Some(i), Some(j)) = (q_index(s), q_index(o)) {
                w[(i, j)] = self.default_weight;
                w[(j, i)] = self.default_weight;
            }
        }
        let mut seen: FxHashSet<(u32, u32, EdgeId)> = FxHashSet::default();
        for &(s, o, e) in &r_graph.edges {
            let adm = match &self.m.edges[e as usize] {
                EdgeEntry::Data(d) => d.admittances.as_ref().map_err(Clone::clone)?,
                _ => unreachable!("r_graph edges are data edges"),
            };
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
                let adm = match &self.m.edges[e as usize] {
                    EdgeEntry::Data(d) => format!("{:?}", d.admittances),
                    _ => String::new(),
                };
                eprintln!("  r_edge ({s}, {o}) {}: admittances {adm}", self.m.edge_names[e as usize]);
            }
            eprintln!("  laplacian (before dropping zero columns) {lap}");
            eprintln!("  kept {kept:?}");
        }
        let pos = |i: usize| kept.iter().position(|&k| k == i).unwrap();
        let probe_inds = probe_rows.iter().map(|&(a, b)| (pos(a), pos(b))).collect();
        let k = kept.len();
        Ok((DMatrix::from_fn(k, k, |i, j| lap[(kept[i], kept[j])]), probe_inds))
    }
}

// --- output --------------------------------------------------------------------------

/// The scored message: the input bytes with the results array rewritten in
/// ranked order and each analysis's score set.
pub fn splice(bytes: &[u8], parsed: &Parsed, ranked: &Ranked) -> Vec<u8> {
    let (Some((start, end)), Some(results)) = (parsed.results_span, &parsed.results) else {
        return bytes.to_vec(); // no results: saved unchanged
    };
    let mut out = Vec::with_capacity(bytes.len() + results.len() * 32);
    out.extend_from_slice(&bytes[..start]);
    out.push(b'[');
    for (k, &i) in ranked.order.iter().enumerate() {
        if k > 0 {
            out.push(b',');
        }
        write_result(&mut out, &results[i], &ranked.finals[i]);
    }
    out.push(b']');
    out.extend_from_slice(&bytes[end..]);
    out
}

fn write_result(out: &mut Vec<u8>, r: &ParsedResult, finals: &[Final]) {
    let raw = r.raw;
    let base = raw.as_ptr() as usize;
    let mut cursor = 0;
    for ((araw, fields), fin) in r.analyses.iter().zip(finals) {
        let off = araw.as_ptr() as usize - base;
        out.extend_from_slice(&raw.as_bytes()[cursor..off]);
        write_analysis(out, araw, fields, *fin);
        cursor = off + araw.len();
    }
    out.extend_from_slice(&raw.as_bytes()[cursor..]);
}

fn write_analysis(out: &mut Vec<u8>, raw: &str, fields: &Thing<AnalysisFields>, fin: Final) {
    let score = match fin {
        Final::Keep(_) | Final::Skip => {
            out.extend_from_slice(raw.as_bytes());
            return;
        }
        f => f.to_value(),
    };
    let has_score = fields.obj().map_or(false, |f| f.score.0.is_some());
    if has_score {
        // Replace the existing score in place (rare: re-scoring a scored message).
        let mut v: Value = serde_json::from_str(raw).expect("analysis JSON");
        v["score"] = score;
        serde_json::to_writer(&mut *out, &v).expect("serialise analysis");
        return;
    }
    debug_assert!(raw.starts_with('{') && raw.ends_with('}'));
    let inner = &raw[1..raw.len() - 1];
    if inner.trim().is_empty() {
        out.extend_from_slice(b"{\"score\":");
    } else {
        out.extend_from_slice(&raw.as_bytes()[..raw.len() - 1]);
        out.extend_from_slice(b",\"score\":");
    }
    serde_json::to_writer(&mut *out, &score).expect("serialise score");
    out.push(b'}');
}
