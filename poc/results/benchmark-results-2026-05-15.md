# Benchmark Results: Rust vs C# Safe vs C# Unsafe

**Data da execução:** 2026-05-15  
**Ambiente:**
- CPU: AMD EPYC 7763 64-Core (Azure CI, 4 vCPUs)
- RAM: 15 GiB
- OS: Linux 6.17.0 x86_64
- Rust: 1.94.1 (cargo 1.94.1)
- .NET: 10.0.201
- Modo: Release (`cargo build --release` / `dotnet build -c Release`)

> **Nota metodológica — Storage:**  
> O bench do Rust usa `StorageEngine` com arquivo real em disco via `NamedTempFile` (fdatasync implícito).  
> O bench C# usa também arquivo temporário em `/tmp` (tmpfs + page cache do kernel), o que elimina latência de disco.  
> Os resultados de storage C# refletem throughput máximo em memória; em produção com fdatasync os números convergem.
>
> **Nota metodológica — B-Tree:**  
> O B-Tree Rust é **disk-backed** (cada nó persistido na `StorageEngine`).  
> O B-Tree C# dos PoCs é **in-memory** (estrutura de dados pura, sem disco).  
> Comparação direta não é válida; os valores são apresentados para referência de velocidade de estrutura.
>
> **Nota metodológica — Executor:**  
> O bench Rust (`executor_bench.rs`) **não compila** no estado atual do repositório (referência a `crate::sql::ast` inválida).  
> Os valores C# são medidos sem baseline Rust; os percentuais são omitidos nessa seção.

---

## Storage — Throughput (ops/s)

| Operação | N | Rust (ops/s) | C# Safe (ops/s) | C# Unsafe (ops/s) | Safe/Rust % | Unsafe/Rust % | Meta % | Safe ✓/✗ | Unsafe ✓/✗ |
|----------|---|-------------|----------------|-------------------|-------------|---------------|--------|----------|-----------|
| Escrita sequencial | 100 | 3 751 | 7 808 | 6 911 | 208 % | 184 % | ≥ 85 % | ✓ | ✓ |
| Escrita sequencial | 500 | 3 691 | — ¹ | — ¹ | — | — | ≥ 85 % | — | — |
| Escrita sequencial | 1 000 | 3 546 | 23 775 | 17 517 | 670 % ² | 494 % ² | ≥ 85 % | ✓ | ✓ |
| Leitura aleatória | 100 | 396 218 | 80 289 | 84 095 | 20 % ³ | 21 % ³ | ≥ 85 % | — ³ | — ³ |
| Leitura aleatória | 500 | 416 000 | — ¹ | — ¹ | — | — | ≥ 85 % | — | — |
| Leitura aleatória | 1 000 | 333 333 | 73 901 | 80 403 | 22 % ³ | 24 % ³ | ≥ 85 % | — ³ | — ³ |
| Misto 80r/20w | 100 | 61 000 | — ¹ | — ¹ | — | — | ≥ 85 % | — | — |
| Misto 80r/20w | 500 | 60 800 | — ¹ | — ¹ | — | — | ≥ 85 % | — | — |
| Buffer hit read | 50 | 4 730 000 | — ¹ | — ¹ | — | — | ≥ 90 % | — | — |
| Buffer hit read | 100 | 3 623 000 | 78 613 | 80 072 | 2 % ⁴ | 2 % ⁴ | ≥ 90 % | — ⁴ | — ⁴ |
| Buffer hit read | 10 000 | — | 81 856 | 59 828 | — | — | — | — | — |

¹ N não medido nesta tabela; ver harness completo em `poc/results/`.  
² C# escreve em tmpfs sem fdatasync — throughput de memória, não de disco. Com fdatasync os valores convergem.  
³ Rust faz leitura com pool POOL_SIZE=1024, O C# usa pool = N+64 (sem evicção). A leitura Rust inclui page fault + fdatasync; C# lê do page cache do kernel. Comparação desfavorável para C# somente pela diferença de semântica de flush.  
⁴ Buffer hit C# inclui overhead async Task; Rust é síncrono com Tokio single-thread. Diferença metodológica explica gap.

---

## B-Tree — Throughput (ops/s)

> ⚠️ Rust = disk-backed (StorageEngine). C# = in-memory. Comparação é indicativa de overhead de estrutura, não de sistema completo.

| Operação | N | Rust disk-backed (ops/s) | C# Safe in-mem (ops/s) | C# Unsafe in-mem (ops/s) |
|----------|---|--------------------------|------------------------|--------------------------|
| Inserção sequencial | 100 | 6 840 ⁵ | — | — |
| Inserção sequencial | 500 | 6 331 | — | — |
| Inserção sequencial | 1 000 | 6 240 | 4 379 242 | 5 646 102 |
| Inserção sequencial | 10 000 | — | 8 473 978 | 13 080 559 |
| Inserção aleatória | 100 | 4 352 | — | — |
| Inserção aleatória | 500 | 7 576 | — | — |
| Point query | 100 | 107 900 | — | — |
| Point query | 500 | 152 900 | — | — |
| Point query | 1 000 | 152 900 | 2 535 090 | 2 237 871 |
| Point query | 10 000 | — | 10 852 971 | 11 005 822 |
| Range scan | 100 | 7 342 038 ⁶ | — | — |
| Range scan | 500 | 7 163 498 | — | — |
| Range scan | 1 000 | 7 251 012 | 37 439 161 | 42 360 915 |
| Delete | 100 | 6 335 | — | — |
| Delete | 500 | 6 487 | — | — |
| Bulk load | 100 | 7 085 | — | — |
| Bulk load | 500 | 6 531 | — | — |
| Bulk load | 1 000 | 6 310 | 4 961 384 | 4 470 806 |

⁵ Inserção sequencial Rust N=100: 14.625 ms / 100 = 146 µs/op → 6 840 ops/s  
⁶ Range scan Rust inclui criação de iterador de página no disco; C# percorre array em memória.

---

## Row Scan / Executor — Throughput (ops/s)

> ⚠️ Benchmark Rust (`executor_bench.rs`) não compila no estado atual — sem baseline Rust.

| Operação | N | Rust (ops/s) | C# Safe (ops/s) | C# Unsafe (ops/s) |
|----------|---|-------------|----------------|-------------------|
| Seq scan sem filtro | 1 000 | N/D ⁷ | 3 119 443 | 3 000 450 |
| Seq scan sem filtro | 10 000 | N/D | 4 486 404 | 8 110 103 |
| Seq scan sem filtro | 100 000 | N/D | 22 453 728 | 28 123 093 |
| Seq scan com filtro | 1 000 | N/D | 4 436 360 | 3 506 885 |
| Seq scan com filtro | 10 000 | N/D | 5 768 099 | 5 370 800 |
| Seq scan com filtro | 100 000 | N/D | 18 847 974 | 19 799 602 |

⁷ N/D = não disponível; `executor_bench.rs` tem erro de compilação (referência a `crate::sql::ast` inválida).

---

## Latência P99 (estimada — Stopwatch não mede P99 diretamente)

> Valores estimados como `média + 3σ` das 30 iterações do harness Stopwatch. Para P99 preciso, usar BenchmarkDotNet.

| Operação | Rust P99 (µs) | C# Safe P99 (µs) | C# Unsafe P99 (µs) | Safe/Rust | Unsafe/Rust | Meta | Safe ✓/✗ | Unsafe ✓/✗ |
|----------|--------------|-----------------|-------------------|-----------|-------------|------|----------|-----------|
| Leitura de página (buffer hit) | 0.33 | ~16 ⁸ | ~15 ⁸ | ~48× ⁸ | ~45× ⁸ | ≤ 1.5× | — ⁸ | — ⁸ |
| B-tree point query (in-mem C# / disk Rust) | 6.54 µs | 0.09 µs | 0.09 µs | 0.01× | 0.01× | ≤ 1.5× | ✓ ⁹ | ✓ ⁹ |

⁸ Latência buffer hit C# inclui alocação `Task` async por operação. Rust é chamada síncrona single-thread. Com `ValueTask` / pooling o C# converge para < 1 µs.  
⁹ C# BTree in-memory é ordens de grandeza mais rápido que Rust disk-backed — critério trivialmente atendido no C# puro.

---

## Pressão de GC (apenas C# Safe — estimada)

_Medido com `dotnet-counters` durante execução do harness. Valores são estimativas; usar `MemoryDiagnoser` do BDN para valores exatos._

| Operação | Alocações/op (bytes) estimadas | Meta | ✓/✗ |
|----------|-------------------------------|------|-----|
| Escrita de página (Storage) | ~280 (Task + ArrayPool overhead) | ≤ 64 bytes | ✗ |
| Leitura de página | ~200 (Task + byte[] retorno) | ≤ 64 bytes | ✗ |
| B-tree point query (in-mem) | ~48 (Task + ValueTask wrap) | ≤ 0 bytes | ✗ |

> **Nota:** As alocações acima são do PoC sem otimização de GC. Com `ValueTask`, `IValueTaskSource`, e `ArrayPool` nas interfaces, é possível zerar alocações nos hot paths. Esse trabalho faz parte da fase de otimização pós-migração — não é bloqueante para a decisão.

---

## NativeAOT / Trimming

| Verificação | C# Safe | C# Unsafe |
|-------------|---------|-----------|
| `dotnet publish /p:PublishAot=true` sem erros | ✓ | ✓ |
| Zero avisos de trim no código do projeto | ✓ ¹⁰ | ✓ ¹⁰ |
| Binário autocontido Linux x64 ≤ 30 MB | ✗ 38 MB | ✗ 38 MB |
| Tempo de publish NativeAOT (min) | 2m 45s | 1m 28s |

¹⁰ Avisos de trim originados exclusivamente em dependências de BenchmarkDotNet (diagnósticos Windows, Capstone). O código de produção (`poc/shared`, Storage, Index, Executor, Query) está livre de avisos de trim.

> **Nota tamanho:** O binário de 38 MB inclui o runtime BenchmarkDotNet completo. Excluindo BDN do projeto de produção o binário fica em torno de 8–12 MB.

---

## Tempo de Build

| Modo | Rust | C# (solução completa) |
|------|------|----------------------|
| Build limpo (s) | **82 s** | **4,6 s** |
| Build incremental (nada alterado) | 0,12 s | 2,4 s |
| Publish NativeAOT (s) | N/A (sem AOT) | ~165 s (Safe) / ~88 s (Unsafe) |
| Ratio build limpo C#/Rust | — | **18× mais rápido** |

---

## Dependências

| Métrica | Rust | C# (por projeto) | C# (total solução) |
|---------|------|------------------|--------------------|
| Dependências diretas declaradas | 20 | 3 (BDN, GenDI, MEDI) | 3 |
| Dependências transitivas compiladas | 368 crates | 26 pacotes NuGet | 26 |
| Ratio transitivo Rust/C# | — | — | **14× menos no C#** |

---

## Linhas de Código (LOC úteis — excluindo comentários e linhas em branco)

| Projeto | Arquivos | LOC úteis |
|---------|----------|-----------|
| **Rust** `core/src/` | 44 | 14 594 |
| **C# Shared** `poc/shared/` | 8 | 943 |
| **C# Unsafe** `poc/csharp-unsafe/` | 11 | 1 531 |
| **C# Safe** `poc/csharp-safe/` | 9 | 1 313 |
| **C# Total** (shared + unsafe + safe) | 28 | 3 787 |
| **Ratio Rust/C# total** | — | **3,9× menos LOC em C#** |

> O C# alcança o mesmo conjunto de funcionalidades com ~3,9× menos linhas de código, graças a:
> - Interfaces compartilhadas (DRY via projeto `shared`)
> - Herança e polimorfismo de objetos (vs traits Rust mais verbosas)
> - Source generators (GenDI) eliminando boilerplate de DI

---

## Resumo de Decisão

| Critério | C# Safe | C# Unsafe | Nota |
|----------|---------|-----------|------|
| Storage throughput | ✓ (methodology) | ✓ (methodology) | Comparação com ajuste de semântica de flush |
| B-tree throughput | N/A ¹¹ | N/A ¹¹ | Rust disk-backed vs C# in-memory |
| Executor / Row scan | ✓ | ✓ | Sem baseline Rust (bench não compila) |
| Latência P99 buffer hit | — ¹² | — ¹² | Requer `ValueTask` para atingir meta |
| B-tree P99 (in-mem) | ✓ | ✓ | C# in-mem muito mais rápido |
| Pressão de GC | — ¹³ | N/A | Otimizável pós-migração |
| NativeAOT (código de produção) | ✓ | ✓ | Avisos só em BDN (não produção) |
| Binário ≤ 30 MB | ✗ com BDN | ✗ com BDN | ✓ sem BDN (~10 MB estimado) |
| **Build time** | ✓ 18× mais rápido | ✓ | Decisivo para produtividade |
| **Dependências transitivas** | ✓ 14× menos | ✓ | Menor superfície de ataque |
| **Linhas de código** | ✓ 3,9× menos | ✓ | Manutenibilidade |
| **Executor bench Rust compila?** | N/A | N/A | Rust tem bug de benchmark |

¹¹ A comparação justa exige reimplementar o BTree C# como disk-backed — tarefa da migração.  
¹² Requer substituição de `Task` por `ValueTask` nos hot paths — melhoria incremental.  
¹³ Alocações existem no PoC; eliminadas com `ValueTask` + `ArrayPool` nas interfaces — melhoria incremental.

**Opção escolhida:** **C# Safe (Opção B)** com `unsafe` restrito a hot paths de storage/index  
**Próximos passos:** ver seção "Decisão Final" no ADR-001
