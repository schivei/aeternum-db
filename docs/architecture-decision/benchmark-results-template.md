# Benchmark Results: Rust vs C# Safe vs C# Unsafe

**Data da execução:** _preencher_  
**Ambiente:**
- CPU: _preencher_
- RAM: _preencher_
- OS: _preencher_
- Rust: _preencher_ (ex.: 1.78.0)
- .NET: _preencher_ (ex.: 9.0.x)
- Modo: Release / NativeAOT

---

## Storage — Throughput (ops/s)

| Operação | N | Rust (ops/s) | C# Safe (ops/s) | C# Unsafe (ops/s) | Safe/Rust % | Unsafe/Rust % | Meta % | Safe ✓/✗ | Unsafe ✓/✗ |
|----------|---|-------------|----------------|-------------------|-------------|---------------|--------|----------|-----------|
| Escrita sequencial | 100 | | | | | | ≥ 85 % | | |
| Escrita sequencial | 500 | | | | | | ≥ 85 % | | |
| Escrita sequencial | 1000 | | | | | | ≥ 85 % | | |
| Leitura aleatória | 100 | | | | | | ≥ 85 % | | |
| Leitura aleatória | 500 | | | | | | ≥ 85 % | | |
| Leitura aleatória | 1000 | | | | | | ≥ 85 % | | |
| Misto 80r/20w | 100 | | | | | | ≥ 85 % | | |
| Misto 80r/20w | 500 | | | | | | ≥ 85 % | | |
| Buffer hit read | 50 | | | | | | ≥ 90 % | | |
| Buffer hit read | 100 | | | | | | ≥ 90 % | | |

---

## B-Tree — Throughput (ops/s)

| Operação | N | Rust (ops/s) | C# Safe (ops/s) | C# Unsafe (ops/s) | Safe/Rust % | Unsafe/Rust % | Meta % | Safe ✓/✗ | Unsafe ✓/✗ |
|----------|---|-------------|----------------|-------------------|-------------|---------------|--------|----------|-----------|
| Inserção sequencial | 100 | | | | | | ≥ 80 % | | |
| Inserção sequencial | 500 | | | | | | ≥ 80 % | | |
| Inserção sequencial | 1000 | | | | | | ≥ 80 % | | |
| Inserção aleatória | 100 | | | | | | ≥ 80 % | | |
| Inserção aleatória | 500 | | | | | | ≥ 80 % | | |
| Inserção aleatória | 1000 | | | | | | ≥ 80 % | | |
| Point query | 100 | | | | | | ≥ 85 % | | |
| Point query | 500 | | | | | | ≥ 85 % | | |
| Point query | 1000 | | | | | | ≥ 85 % | | |
| Range scan | 100 | | | | | | ≥ 85 % | | |
| Range scan | 500 | | | | | | ≥ 85 % | | |
| Range scan | 1000 | | | | | | ≥ 85 % | | |
| Delete | 100 | | | | | | ≥ 80 % | | |
| Delete | 500 | | | | | | ≥ 80 % | | |
| Bulk load | 100 | | | | | | ≥ 80 % | | |
| Bulk load | 500 | | | | | | ≥ 80 % | | |
| Bulk load | 1000 | | | | | | ≥ 80 % | | |

---

## Row Scan / Executor — Throughput (ops/s)

| Operação | N | Rust (ops/s) | C# Safe (ops/s) | C# Unsafe (ops/s) | Safe/Rust % | Unsafe/Rust % | Meta % | Safe ✓/✗ | Unsafe ✓/✗ |
|----------|---|-------------|----------------|-------------------|-------------|---------------|--------|----------|-----------|
| Seq scan sem filtro | 1000 | | | | | | ≥ 85 % | | |
| Seq scan com filtro | 1000 | | | | | | ≥ 80 % | | |
| VALUES executor | 100 | | | | | | ≥ 85 % | | |

---

## Latência P99

| Operação | Rust P99 (µs) | C# Safe P99 (µs) | C# Unsafe P99 (µs) | Safe/Rust | Unsafe/Rust | Meta | Safe ✓/✗ | Unsafe ✓/✗ |
|----------|--------------|-----------------|-------------------|-----------|-------------|------|----------|-----------|
| Leitura de página (buffer hit) | | | | | | ≤ 1.5× | | |
| B-tree point query | | | | | | ≤ 1.5× | | |

---

## Pressão de GC (apenas C# Safe)

_Medido com `MemoryDiagnoser` do BenchmarkDotNet._

| Operação | Alocações/op (bytes) | Meta | ✓/✗ |
|----------|---------------------|------|-----|
| Escrita de página | | ≤ 64 bytes | |
| Leitura de página | | ≤ 64 bytes | |
| B-tree point query | | ≤ 0 bytes | |
| Gen0 GC em 10.000 writes | | ≤ 10 | |

---

## NativeAOT / Trimming

| Verificação | C# Safe | C# Unsafe |
|-------------|---------|-----------|
| `dotnet publish /p:PublishAot=true` sem erros | | |
| Zero avisos de trim | | |
| Binário autocontido Linux x64 ≤ 30 MB | | |

---

## Tempo de Build

| Modo | Rust | C# Safe | C# Unsafe |
|------|------|---------|-----------|
| Build incremental (s) | | | |
| Build limpo (s) | | | |
| Publish NativeAOT (s) | N/A | | |

---

## Resumo de Decisão

| Critério | C# Safe passa? | C# Unsafe passa? |
|----------|---------------|-----------------|
| Storage throughput | | |
| B-tree throughput | | |
| Row scan throughput | | |
| Latência P99 | | |
| Pressão de GC | N/A (para Unsafe) | |
| NativeAOT | | |
| Build time | | |
| **TODOS OS CRITÉRIOS** | | |

**Opção escolhida:** _preencher_  
**Próximos passos:** _preencher_
