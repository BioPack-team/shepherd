//! The Kirchhoff index (summed effective resistance between the probes) of a
//! reduced Laplacian, as `kirchhoff` in the Python worker computes it:
//! trace(X^T lstsq(L, X)) with numpy's `rcond=None` cutoff.
//!
//! Two solvers:
//!
//! * `Svd`: the least-squares solution through faer's SVD, exactly what
//!   `lstsq` does. A decomposition that doesn't reconstruct L is an error,
//!   not a wrong score (nalgebra's decompositions were found to fail silently
//!   on some of these matrices).
//! * `Cholesky`: when every probe is in one connected component C of the
//!   graph and C's rows of L form a true Laplacian (zero row sums: no
//!   self-loops), the effective resistance between two nodes of C is
//!   (e_a - e_b)^T L_g^-1 (e_a - e_b) with L_g = C's Laplacian minus one
//!   grounded node, which is symmetric positive definite: one small Cholesky
//!   factorisation instead of an SVD of the whole matrix. In exact arithmetic
//!   it is the same number lstsq returns (e_a - e_b is in L's range, so the
//!   least-squares solution is exact). Anything else -- probes in different
//!   components, self-loops, a non-positive pivot -- falls back to the SVD.

use nalgebra::DMatrix;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Solver {
    Svd,
    Cholesky,
}

impl Solver {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "svd" => Some(Solver::Svd),
            "cholesky" => Some(Solver::Cholesky),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Solver::Svd => "svd",
            Solver::Cholesky => "cholesky",
        }
    }
}

/// How often each path was taken, for the run summary.
#[derive(Default, Clone, Copy, Debug)]
pub struct SolveStats {
    pub cholesky: usize,
    pub svd: usize,
    pub fallbacks: usize,
}

pub fn kirchhoff(
    l: &DMatrix<f64>,
    probes: &[(usize, usize)],
    solver: Solver,
    stats: &mut SolveStats,
) -> Result<f64, String> {
    if probes.is_empty() {
        return Ok(f64::NEG_INFINITY); // np.stack([]) raises -> -inf
    }
    if l.iter().any(|v| !v.is_finite()) {
        return Err("SVD did not converge in Linear Least Squares".into());
    }
    if solver == Solver::Cholesky {
        if let Some(k) = kirchhoff_grounded(l, probes) {
            stats.cholesky += 1;
            return Ok(k);
        }
        stats.fallbacks += 1;
    }
    stats.svd += 1;
    kirchhoff_svd(l, probes)
}

fn kirchhoff_grounded(l: &DMatrix<f64>, probes: &[(usize, usize)]) -> Option<f64> {
    let n = l.nrows();
    // Connected components over the non-zero off-diagonal entries.
    let mut parent: Vec<usize> = (0..n).collect();
    fn find(p: &mut [usize], mut x: usize) -> usize {
        while p[x] != x {
            p[x] = p[p[x]];
            x = p[x];
        }
        x
    }
    for i in 0..n {
        for j in i + 1..n {
            if l[(i, j)] != 0.0 {
                let (a, b) = (find(&mut parent, i), find(&mut parent, j));
                if a != b {
                    parent[a] = b;
                }
            }
        }
    }
    let root = find(&mut parent, probes[0].0);
    for &(a, b) in probes {
        if find(&mut parent, a) != root || find(&mut parent, b) != root {
            return None; // probes in different components: lstsq's answer isn't R_ab
        }
    }
    let comp: Vec<usize> = (0..n).filter(|&i| find(&mut parent, i) == root).collect();

    // A true Laplacian has zero row sums; a self-loop (an edge or q_edge
    // from an r_node to itself) puts weight on the diagonal only.
    let scale = comp.iter().fold(0.0f64, |m, &i| m.max(l[(i, i)].abs())).max(1e-300);
    for &i in &comp {
        let row: f64 = comp.iter().map(|&j| l[(i, j)]).sum();
        if row.abs() > 1e-9 * scale {
            return None;
        }
    }

    // Ground the component's last node; Cholesky-factor the rest.
    let m = comp.len() - 1;
    if m == 0 {
        return None;
    }
    let mut a = vec![0.0f64; m * m];
    for (r, &i) in comp[..m].iter().enumerate() {
        for (c, &j) in comp[..m].iter().enumerate() {
            a[r * m + c] = l[(i, j)];
        }
    }
    for j in 0..m {
        let mut d = a[j * m + j];
        for k in 0..j {
            d -= a[j * m + k] * a[j * m + k];
        }
        if !(d > 0.0) || !d.is_finite() {
            return None;
        }
        let d = d.sqrt();
        a[j * m + j] = d;
        for i in j + 1..m {
            let mut s = a[i * m + j];
            for k in 0..j {
                s -= a[i * m + k] * a[j * m + k];
            }
            a[i * m + j] = s / d;
        }
    }

    let pos = |node: usize| comp[..m].iter().position(|&c| c == node);
    let mut total = 0.0;
    let mut b = vec![0.0f64; m];
    for &(pa, pb) in probes {
        b.iter_mut().for_each(|v| *v = 0.0);
        if let Some(i) = pos(pa) {
            b[i] = -1.0;
        }
        if let Some(i) = pos(pb) {
            b[i] = 1.0;
        }
        // L_g z = b: forward (L y = b), then back (L^T z = y).
        let mut y = b.clone();
        for i in 0..m {
            let mut s = y[i];
            for k in 0..i {
                s -= a[i * m + k] * y[k];
            }
            y[i] = s / a[i * m + i];
        }
        for i in (0..m).rev() {
            let mut s = y[i];
            for k in i + 1..m {
                s -= a[k * m + i] * y[k];
            }
            y[i] = s / a[i * m + i];
        }
        total += b.iter().zip(&y).map(|(u, v)| u * v).sum::<f64>();
    }
    Some(total)
}

fn kirchhoff_svd(l: &DMatrix<f64>, probes: &[(usize, usize)]) -> Result<f64, String> {
    let n = l.nrows();
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
    let mut total = 0.0;
    for &(pa, pb) in probes {
        // x = e_pb - e_pa (e_pb alone if they coincide); x^T V S+ U^T x
        for k in 0..n {
            if s[k] > cutoff {
                let (ut_x, vt_x) = if pa == pb {
                    (u[(pb, k)], v[(pb, k)])
                } else {
                    (u[(pb, k)] - u[(pa, k)], v[(pb, k)] - v[(pa, k)])
                };
                total += vt_x * ut_x / s[k];
            }
        }
    }
    Ok(total)
}
