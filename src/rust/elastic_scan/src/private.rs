use super::*;

pub enum ElasticRequestKind {
    First {
        index_url_part: String,
        query: Value,
        fields: Vec<String>,
        fetch_limit: usize,
    },
    Next {
        scroll_id: String,
        prev_fetch_start: SystemTime,
        prev_fetch_end: SystemTime,
        processed_count: usize,
        scan_start: SystemTime,
    },
}

impl ElasticRequestKind {
    pub fn processed_count(&self) -> usize {
        match self {
            Self::First { .. } => 0,
            Self::Next {
                processed_count, ..
            } => *processed_count,
        }
    }
    pub fn scan_start(&self) -> Option<SystemTime> {
        match self {
            Self::First { .. } => None,
            Self::Next { scan_start, .. } => Some(*scan_start),
        }
    }
}
