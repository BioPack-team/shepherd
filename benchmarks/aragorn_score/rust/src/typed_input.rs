//! Typed, borrowed views of the parts of a TRAPI message the ranker reads
//! (`--parse typed`).
//!
//! Hand-written serde visitors instead of `serde_json::Value`:
//!
//! * only the fields the ranker reads are kept; everything else is skipped
//!   without allocating (`IgnoredAny`);
//! * strings borrow from the input buffer (`Cow`, owned only when escaped);
//! * an attribute's `value` is kept as raw JSON (`Raw`) and only
//!   interpreted for the attributes scoring looks at; a list is reduced to
//!   its length and first element (`AV::List`), so a publications list of
//!   thousands of PMIDs is counted, not built;
//! * results and analyses are kept as raw JSON slices too, so the scored
//!   message can be written by splicing scores into the input bytes.
//!
//! Python's behaviour on odd shapes (a null where a list belongs, a node
//! that isn't an object) is a per-answer error at the point it's used, not a
//! parse failure, so values that could be odd are recorded as such (`Thing`,
//! `Maybe`) and judged where the Python code would trip over them. Shapes
//! the visitors can't represent at all fail the typed parse, and the runner
//! then falls back to the `Value` path for that message.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, Deserialize, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::cell::Cell;
use std::ops::Deref;

pub type R<T> = Result<T, String>;

// --- parser backend --------------------------------------------------------------------

/// Which JSON parser drives the visitors below (`--json`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    SerdeJson,
    /// sonic-rs through the same serde visitors.
    Sonic,
}

impl Backend {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "serde_json" => Some(Backend::SerdeJson),
            "sonic" | "sonic-rs" => Some(Backend::Sonic),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Backend::SerdeJson => "serde_json",
            Backend::Sonic => "sonic",
        }
    }
}

thread_local! {
    static BACKEND: Cell<Backend> = const { Cell::new(Backend::SerdeJson) };
}

/// Deserialize from a borrowed &str with the current backend.
fn from_str<'de, T: Deserialize<'de>>(s: &'de str) -> R<T> {
    match BACKEND.with(Cell::get) {
        Backend::SerdeJson => serde_json::from_str(s).map_err(|e| e.to_string()),
        Backend::Sonic => sonic_rs::from_str(s).map_err(|e| e.to_string()),
    }
}

/// A value's raw JSON text, borrowed from the input. Each backend has its own
/// raw-value type (serde_json's `RawValue`, sonic-rs's `LazyValue`), which
/// only its own deserializer recognises, so this asks for whichever is
/// running.
#[derive(Clone, Copy)]
pub struct Raw<'de>(pub &'de str);

impl<'de> Deserialize<'de> for Raw<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match BACKEND.with(Cell::get) {
            Backend::SerdeJson => {
                Ok(Raw(<&'de serde_json::value::RawValue>::deserialize(d)?.get()))
            }
            Backend::Sonic => match sonic_rs::LazyValue::<'de>::deserialize(d)?.as_raw_cow() {
                Cow::Borrowed(s) => Ok(Raw(s)),
                Cow::Owned(_) => Err(de::Error::custom("sonic-rs returned an owned raw value")),
            },
        }
    }
}

/// A string (an object key, mostly) borrowed from the input when it has no
/// escapes. serde's own `Cow<str>` always allocates a `String`.
pub struct Key<'de>(pub Cow<'de, str>);

impl Deref for Key<'_> {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct KV;
        impl<'de> Visitor<'de> for KV {
            type Value = Key<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string")
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Borrowed(v)))
            }
            fn visit_str<E>(self, v: &str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(v.to_owned())))
            }
            fn visit_string<E>(self, v: String) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(v)))
            }
        }
        d.deserialize_str(KV)
    }
}

// --- AV: a JSON value reduced to what Python's checks need --------------------

#[derive(Clone, Debug)]
pub enum AV<'a> {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(Cow<'a, str>),
    /// A list: its length and first element only.
    List {
        len: usize,
        first: Option<Box<AV<'a>>>,
    },
    /// An object: its number of keys only.
    Obj {
        len: usize,
    },
}

impl<'a> AV<'a> {
    /// Python truthiness.
    pub fn truthy(&self) -> bool {
        match self {
            AV::Null => false,
            AV::Bool(b) => *b,
            AV::Int(i) => *i != 0,
            AV::UInt(u) => *u != 0,
            AV::Float(f) => *f != 0.0,
            AV::Str(s) => !s.is_empty(),
            AV::List { len, .. } | AV::Obj { len } => *len > 0,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            AV::Null => "None",
            AV::Bool(_) => "bool",
            AV::Int(_) | AV::UInt(_) | AV::Float(_) => "number",
            AV::Str(_) => "str",
            AV::List { .. } => "list",
            AV::Obj { .. } => "dict",
        }
    }

    /// A value Python can do arithmetic on (int, float, bool).
    pub fn as_num(&self) -> R<f64> {
        match self {
            AV::Int(i) => Ok(*i as f64),
            AV::UInt(u) => Ok(*u as f64),
            AV::Float(f) => Ok(*f),
            AV::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            other => Err(format!("unsupported operand type: {}", other.type_name())),
        }
    }

    /// Python `int(v)`, ValueError -> 0 (as every caller does), TypeError /
    /// OverflowError -> error.
    pub fn py_int_or_zero(&self) -> R<f64> {
        match self {
            AV::Int(i) => Ok(*i as f64),
            AV::UInt(u) => Ok(*u as f64),
            AV::Float(f) if f.is_nan() => Ok(0.0),
            AV::Float(f) if f.is_infinite() => Err("cannot convert float infinity to integer".into()),
            AV::Float(f) => Ok(f.trunc()),
            AV::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            AV::Str(s) => Ok(s.trim().parse::<i64>().map(|i| i as f64).unwrap_or(0.0)),
            other => Err(format!("int() argument must not be {}", other.type_name())),
        }
    }
}

/// `x.get(key, "")` then `if not x: x = ""`, used as a string.
pub fn name_str<'x>(v: &'x Has<AV>) -> R<&'x str> {
    match &v.0 {
        Some(AV::Str(s)) => Ok(s),
        Some(v) if v.truthy() => Err(format!("attribute name is a {}, not a str", v.type_name())),
        _ => Ok(""),
    }
}

impl<'de> Deserialize<'de> for AV<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(AVVisitor)
    }
}

struct AVVisitor;

impl<'de> Visitor<'de> for AVVisitor {
    type Value = AV<'de>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_unit<E>(self) -> Result<AV<'de>, E> {
        Ok(AV::Null)
    }
    fn visit_none<E>(self) -> Result<AV<'de>, E> {
        Ok(AV::Null)
    }
    fn visit_bool<E>(self, v: bool) -> Result<AV<'de>, E> {
        Ok(AV::Bool(v))
    }
    fn visit_i64<E>(self, v: i64) -> Result<AV<'de>, E> {
        Ok(AV::Int(v))
    }
    fn visit_u64<E>(self, v: u64) -> Result<AV<'de>, E> {
        Ok(AV::UInt(v))
    }
    fn visit_f64<E>(self, v: f64) -> Result<AV<'de>, E> {
        Ok(AV::Float(v))
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<AV<'de>, E> {
        Ok(AV::Str(Cow::Borrowed(v)))
    }
    fn visit_str<E>(self, v: &str) -> Result<AV<'de>, E> {
        Ok(AV::Str(Cow::Owned(v.to_owned())))
    }
    fn visit_string<E>(self, v: String) -> Result<AV<'de>, E> {
        Ok(AV::Str(Cow::Owned(v)))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<AV<'de>, A::Error> {
        let first = seq.next_element::<AV<'de>>()?;
        let mut len = first.is_some() as usize;
        while seq.next_element::<IgnoredAny>()?.is_some() {
            len += 1;
        }
        Ok(AV::List {
            len,
            first: first.map(Box::new),
        })
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<AV<'de>, A::Error> {
        let mut len = 0;
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {
            len += 1;
        }
        Ok(AV::Obj { len })
    }
}

/// An attribute value, kept raw until something needs it.
#[derive(Clone, Copy)]
pub enum Lazy<'a> {
    Missing,
    Raw(Raw<'a>),
}

impl<'a> Lazy<'a> {
    /// None when the key was absent.
    pub fn av(&self) -> Option<AV<'a>> {
        match self {
            Lazy::Missing => None,
            Lazy::Raw(r) => serde_json::from_str::<AV<'a>>(r.0).ok(),
        }
    }
    pub fn full(&self) -> Option<serde_json::Value> {
        match self {
            Lazy::Missing => None,
            Lazy::Raw(r) => serde_json::from_str(r.0).ok(),
        }
    }
}

// --- containers ----------------------------------------------------------------

/// A key that was absent, null, or present.
#[derive(Default)]
pub enum Maybe<T> {
    #[default]
    Missing,
    Null,
    Present(T),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Maybe<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match Option::<T>::deserialize(d)? {
            None => Maybe::Null,
            Some(v) => Maybe::Present(v),
        })
    }
}

/// A JSON object's entries in document order (duplicates kept; the last one
/// wins where they're looked up, as in Python).
pub struct Entries<K, V>(pub Vec<(K, V)>);

impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for Entries<K, V> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V2<K, V>(PhantomData<(K, V)>);
        impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Visitor<'de> for V2<K, V> {
            type Value = Entries<K, V>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(kv) = map.next_entry()? {
                    out.push(kv);
                }
                Ok(Entries(out))
            }
        }
        d.deserialize_map(V2(PhantomData))
    }
}

/// An object's fields of interest, filled one key at a time.
pub trait Fields<'de>: Default {
    fn field<A: MapAccess<'de>>(&mut self, key: &str, map: &mut A) -> Result<(), A::Error>;
}

/// A value that should be an object: the object's fields (and its number of
/// keys, for Python's `if not x`), or a note that it was a falsy or truthy
/// non-object.
pub enum Thing<T> {
    Obj { f: T, len: usize },
    Falsy,
    Bad(&'static str),
}

impl<T> Thing<T> {
    pub fn truthy(&self) -> bool {
        match self {
            Thing::Obj { len, .. } => *len > 0,
            Thing::Falsy => false,
            Thing::Bad(_) => true,
        }
    }
    /// The fields, if it is an object at all (empty or not).
    pub fn obj(&self) -> Option<&T> {
        match self {
            Thing::Obj { f, .. } => Some(f),
            _ => None,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Thing::Obj { .. } => "dict",
            Thing::Falsy => "a falsy value",
            Thing::Bad(t) => t,
        }
    }
}

impl<'de, T: Fields<'de>> Deserialize<'de> for Thing<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct TV<T>(PhantomData<T>);
        impl<'de, T: Fields<'de>> Visitor<'de> for TV<T> {
            type Value = Thing<T>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Thing<T>, A::Error> {
                let mut f = T::default();
                let mut len = 0;
                while let Some(key) = map.next_key::<Key<'de>>()? {
                    len += 1;
                    f.field(&key, &mut map)?;
                }
                Ok(Thing::Obj { f, len })
            }
            fn visit_unit<E>(self) -> Result<Thing<T>, E> {
                Ok(Thing::Falsy)
            }
            fn visit_none<E>(self) -> Result<Thing<T>, E> {
                Ok(Thing::Falsy)
            }
            fn visit_bool<E>(self, v: bool) -> Result<Thing<T>, E> {
                Ok(if v { Thing::Bad("bool") } else { Thing::Falsy })
            }
            fn visit_i64<E>(self, v: i64) -> Result<Thing<T>, E> {
                Ok(if v != 0 { Thing::Bad("int") } else { Thing::Falsy })
            }
            fn visit_u64<E>(self, v: u64) -> Result<Thing<T>, E> {
                Ok(if v != 0 { Thing::Bad("int") } else { Thing::Falsy })
            }
            fn visit_f64<E>(self, v: f64) -> Result<Thing<T>, E> {
                Ok(if v != 0.0 { Thing::Bad("float") } else { Thing::Falsy })
            }
            fn visit_str<E>(self, v: &str) -> Result<Thing<T>, E> {
                Ok(if v.is_empty() { Thing::Falsy } else { Thing::Bad("str") })
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Thing<T>, A::Error> {
                let mut len = 0;
                while seq.next_element::<IgnoredAny>()?.is_some() {
                    len += 1;
                }
                Ok(if len > 0 { Thing::Bad("list") } else { Thing::Falsy })
            }
        }
        d.deserialize_any(TV(PhantomData))
    }
}

macro_rules! fields {
    ($name:ident { $($key:literal => $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Default)]
        pub struct $name<'de> {
            $(pub $field: $ty,)*
            _p: PhantomData<&'de ()>,
        }
        impl<'de> Fields<'de> for $name<'de> {
            fn field<A: MapAccess<'de>>(&mut self, key: &str, map: &mut A) -> Result<(), A::Error> {
                match key {
                    $($key => self.$field = map.next_value()?,)*
                    _ => { map.next_value::<IgnoredAny>()?; }
                }
                Ok(())
            }
        }
    };
}

/// A field that is `Some` once its key is seen -- even when the value is
/// null (unlike `Option<T>`, which serde maps null to `None`): Python tells
/// a missing key from a null one (`edge["subject"]` raises only on missing).
pub struct Has<T>(pub Option<T>);

impl<T> Default for Has<T> {
    fn default() -> Self {
        Has(None)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Has<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Has(Some(T::deserialize(d)?)))
    }
}

pub type Opt<'de> = Has<AV<'de>>;

fields!(AttrFields {
    "original_attribute_name" => orig: Opt<'de>,
    "attribute_type_id" => type_id: Opt<'de>,
    "value" => value: LazyField<'de>,
});

fields!(SourceFields {
    "resource_role" => role: Opt<'de>,
    "resource_id" => id: Opt<'de>,
});

fields!(EdgeFields {
    "subject" => subject: Opt<'de>,
    "object" => object: Opt<'de>,
    "predicate" => predicate: Opt<'de>,
    "sources" => sources: Maybe<Vec<Thing<SourceFields<'de>>>>,
    "attributes" => attributes: Maybe<Vec<Thing<AttrFields<'de>>>>,
});

fields!(NodeFields {
    "attributes" => attributes: Maybe<Vec<Thing<AttrFields<'de>>>>,
});

fields!(AuxFields {
    "nodes" => nodes: Maybe<Vec<AV<'de>>>,
    "edges" => edges: Maybe<Vec<AV<'de>>>,
});

fields!(QEdgeFields {
    "subject" => subject: Opt<'de>,
    "object" => object: Opt<'de>,
});

fields!(QGraphFields {
    "nodes" => nodes: Maybe<Entries<Key<'de>, IgnoredAny>>,
    "edges" => edges: Maybe<Entries<Key<'de>, Thing<QEdgeFields<'de>>>>,
});

fields!(KGraphFields {
    "nodes" => nodes: Maybe<Entries<Key<'de>, Thing<NodeFields<'de>>>>,
    "edges" => edges: Maybe<Entries<Key<'de>, Thing<EdgeFields<'de>>>>,
});

fields!(MessageFields {
    "query_graph" => query_graph: Has<Thing<QGraphFields<'de>>>,
    "knowledge_graph" => knowledge_graph: Has<Thing<KGraphFields<'de>>>,
    "auxiliary_graphs" => auxiliary_graphs: Maybe<Entries<Key<'de>, Thing<AuxFields<'de>>>>,
    "results" => results: Option<Raw<'de>>,
});

fields!(BindingFields {
    "ids" => ids: Maybe<Vec<AV<'de>>>,
});

fields!(ResultFields {
    "node_bindings" => node_bindings: Maybe<Entries<Key<'de>, Thing<BindingFields<'de>>>>,
    "analyses" => analyses: Maybe<Vec<Raw<'de>>>,
});

fields!(AnalysisFields {
    "edge_bindings" => edge_bindings: Maybe<Entries<Key<'de>, Thing<BindingFields<'de>>>>,
    "support_graphs" => support_graphs: Maybe<Vec<AV<'de>>>,
    "score" => score: Opt<'de>,
});

/// An attribute's `value`, raw until read.
pub struct LazyField<'de>(pub Lazy<'de>);

impl Default for LazyField<'_> {
    fn default() -> Self {
        LazyField(Lazy::Missing)
    }
}

impl<'de> Deserialize<'de> for LazyField<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(LazyField(Lazy::Raw(Raw::deserialize(d)?)))
    }
}

/// The parsed input: the message's fields, plus each result and analysis
/// with its raw JSON (for splicing scores in).
pub struct Parsed<'de> {
    pub message: MessageFields<'de>,
    pub results: Option<Vec<ParsedResult<'de>>>,
    /// Byte range of the results array in the input.
    pub results_span: Option<(usize, usize)>,
}

pub struct ParsedResult<'de> {
    pub raw: &'de str,
    pub fields: Thing<ResultFields<'de>>,
    pub analyses: Vec<(&'de str, Thing<AnalysisFields<'de>>)>,
}

fn span(base: &[u8], part: &str) -> (usize, usize) {
    let start = part.as_ptr() as usize - base.as_ptr() as usize;
    (start, start + part.len())
}

/// Parse a stored TRAPI Response (`{"message": ...}`) or a bare Message.
/// The top level, read in one pass: a stored Response's `"message"`, or a
/// bare Message's own fields.
#[derive(Default)]
struct TopFields<'de> {
    message: Has<Thing<MessageFields<'de>>>,
    bare: MessageFields<'de>,
}

impl<'de> Fields<'de> for TopFields<'de> {
    fn field<A: MapAccess<'de>>(&mut self, key: &str, map: &mut A) -> Result<(), A::Error> {
        match key {
            "message" => self.message = map.next_value()?,
            _ => self.bare.field(key, map)?,
        }
        Ok(())
    }
}

pub fn parse(bytes: &[u8], backend: Backend) -> Result<Parsed<'_>, String> {
    BACKEND.with(|b| b.set(backend));
    // Validate UTF-8 once, up front, with SIMD (several times faster than
    // std's check), then parse from &str: from_slice would re-validate every
    // string it reads.
    let text = simdutf8::basic::from_utf8(bytes).map_err(|_| "not UTF-8".to_string())?;
    let top: Thing<TopFields> = from_str(text).map_err(|e| format!("message: {e}"))?;
    let top = match top {
        Thing::Obj { f, .. } => f,
        other => return Err(format!("the document is {}", other.kind())),
    };
    // A Response's "message" when it is an object, else the document itself.
    let message = match top.message.0 {
        Some(Thing::Obj { f, .. }) => f,
        _ => top.bare,
    };

    finish(bytes, message)
}

/// Parse the results (raw, typed, and each analysis) of a parsed message.
fn finish<'de>(bytes: &[u8], message: MessageFields<'de>) -> R<Parsed<'de>> {
    let mut results = None;
    let mut results_span = None;
    if let Some(raw) = message.results.filter(|r| r.0.starts_with('[')) {
        results_span = Some(span(bytes, raw.0));
        let raws: Vec<Raw> = from_str(raw.0).map_err(|e| format!("results: {e}"))?;
        let mut out = Vec::with_capacity(raws.len());
        for r in raws {
            let fields: Thing<ResultFields> =
                from_str(r.0).map_err(|e| format!("a result: {e}"))?;
            let mut analyses = vec![];
            if let Thing::Obj { f, .. } = &fields {
                if let Maybe::Present(list) = &f.analyses {
                    for a in list {
                        let af: Thing<AnalysisFields> =
                            from_str(a.0).map_err(|e| format!("an analysis: {e}"))?;
                        analyses.push((a.0, af));
                    }
                }
            }
            out.push(ParsedResult {
                raw: r.0,
                fields,
                analyses,
            });
        }
        results = Some(out);
    }
    Ok(Parsed {
        message,
        results,
        results_span,
    })
}
