# onpair

OnPair is a dictionary-based string compression algorithm designed for on-disk and in-memory database workloads that need both strong compression ratios and fast random access to individual values. 
It builds its dictionary in a single sequential pass by incrementally merging frequent adjacent substrings, achieving compression comparable to BPE while being substantially faster and more memory-efficient. 

## Interchange format

OnPair defines a shared in-memory representation — the *plain interchange form*
that independent implementations exchange so a column produced by one is
readable by another. It fixes the buffers (dictionary bytes, dictionary
offsets, codes, and row offsets) and their invariants; denser internal
encodings and on-disk serialization are out of scope. See
[docs/interchange-format.md](docs/interchange-format.md).

## References

- Paper: Francesco Gargiulo et al., *OnPair: Short Strings Compression for Fast Random Access* — [arXiv:2508.02280](https://arxiv.org/abs/2508.02280)
- Reference C++ implementation: [gargiulofrancesco/onpair_cpp](https://github.com/gargiulofrancesco/onpair_cpp)
