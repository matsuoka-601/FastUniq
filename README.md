# FastUniq
`FastUniq` is a header-only library for extremely fast string deduplication using a parallelized hash table. 

`sort <input file> | uniq` is often used as a one liner for string deduplication. 
But if your goal is only deduplication and you don't need a sorted output, you can go much faster! 
In this repository, we investigate how much faster a string uniquifier that implements the following optimization can be than `sort <input file> | uniq` :

- Parallelized hash table that scales well with the number of threads
- Fast hash function using SSE/AVX2
- Batching hash calculation and insertion into hash table

## How to use `FastUniq` in your program
In order to use `FastUniq` in your program, include `FastUniq.hpp` and give `-mavx2, -maes, -fopenmp` flags to your compiler when compiling the program. For maximum performance, it's recommended to give either `-O3` or `-Ofast`.

Example: 
```
g++ your_program.cpp -mavx2 -maes -O3 -fopenmp
```
