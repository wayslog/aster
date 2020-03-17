pub(in crate::utils) mod normal;

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "avx2"
))]
mod simd;

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "avx2"
))]
pub use simd::find_lf_simd;

#[cfg(not(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "avx2"
)))]
pub use normal::find_lf_iter as find_lf_simd;
