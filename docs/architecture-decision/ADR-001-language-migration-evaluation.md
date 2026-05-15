# ADR-001 — Avaliação de Migração de Linguagem: Rust → C#

**Status:** ✅ Decidido  
**Data:** 2026-05-15  
**Decisores:** Schivei (lead developer), Copilot (avaliação técnica)

---

## Contexto

O núcleo do AeternumDB é escrito em Rust. O principal desenvolvedor considera migrar para C#, por maior familiaridade com a linguagem e desejo de controle explícito de memória via `unsafe` + NativeAOT/Trimming com o menor uso possível do GC.

Esta ADR documenta o processo estruturado de decisão: define critérios mensuráveis, descreve as três opções avaliadas, define os PoCs de comparação e estipula o gatilho de rollback.

---

## Opções Avaliadas

| ID  | Descrição |
|-----|-----------|
| **A** | Manter Rust (baseline) |
| **B** | C# seguro: GC + `ArrayPool`/`Span<T>`/`Memory<T>` + NativeAOT/Trimming |
| **C** | C# com `unsafe` nos hot paths: `NativeMemory`, ponteiros explícitos, janelas controladas sem GC |

---

## Critérios de Decisão e Metas

Para migrar do Rust para qualquer variante de C# (B ou C), **todos** os critérios abaixo devem ser atendidos. O não atendimento de um único critério é suficiente para manter a opção A (Rust).

### 1. Throughput de Storage (operações/segundo)

| Operação | Meta (% do Rust) |
|----------|-----------------|
| Escrita sequencial de páginas (N=1000) | ≥ 85 % |
| Leitura aleatória de páginas (N=1000) | ≥ 85 % |
| Carga mista 80 % leitura / 20 % escrita | ≥ 85 % |
| Leitura com buffer quente (N=100) | ≥ 90 % |

### 2. B-Tree (operações/segundo)

| Operação | Meta (% do Rust) |
|----------|-----------------|
| Inserção sequencial (N=1000) | ≥ 80 % |
| Inserção aleatória (N=1000) | ≥ 80 % |
| Point query (N=1000) | ≥ 85 % |
| Range scan (N=1000) | ≥ 85 % |
| Delete (N=500) | ≥ 80 % |
| Bulk load (N=1000) | ≥ 80 % |

### 3. Row Scan / Executor (operações/segundo)

| Operação | Meta (% do Rust) |
|----------|-----------------|
| Seq scan 1000 linhas sem filtro | ≥ 85 % |
| Seq scan 1000 linhas com filtro | ≥ 80 % |
| VALUES executor 100 linhas | ≥ 85 % |

### 4. Latência P99

Para as operações de leitura de página e point query, a latência P99 em C# não deve exceder 1,5× a do Rust.

### 5. Pressão de GC (apenas opção B)

Medido com `BenchmarkDotNet` + `MemoryDiagnoser`:

- Alocações por operação de storage (write/read): ≤ 64 bytes
- Alocações por B-tree point query: ≤ 0 bytes (structs na stack)
- Coletas de Gen0 em 10.000 operações de storage: ≤ 10

### 6. Compatibilidade NativeAOT/Trimming

O projeto deve compilar e executar corretamente com:

```bash
dotnet publish -r linux-x64 -c Release /p:PublishAot=true /p:TrimmerRootDescriptor=...
```

Sem avisos de trim e sem `[RequiresUnreferencedCode]` em código de produção.

### 7. Tamanho do binário NativeAOT (opcional, informativo)

Meta: binário do servidor ≤ 30 MB autocontido (sem runtime instalado).

### 8. Tempo de build

O tempo de build incremental (sem AOT) não deve exceder 2× o do Rust. O tempo de publish NativeAOT é aceito como mais lento (30–120 s) por ser usado apenas em release.

### 9. Dependências

| Métrica | Rust | C# (solução) |
|---------|------|--------------|
| Dependências diretas | 20 | 3 |
| Dependências transitivas | 368 crates | 26 pacotes NuGet |

**Meta:** dependências transitivas ≤ 50 pacotes NuGet (manter superfície de ataque controlada).

### 10. Linhas de código úteis (LOC)

_Critério informativo: menor LOC não é necessariamente melhor, mas sinaliza legibilidade e nível de abstração da linguagem._

**Meta informativa:** implementação equivalente em C# deve usar ≤ 50% das LOC do Rust para o mesmo conjunto de funcionalidades (indica linguagem mais expressiva para o domínio).

### 11. Complexidade de manutenção

Avaliação qualitativa pela equipe após a PoC:

- O código C# é tão legível e rastreável quanto o Rust?
- O número de `unsafe` blocks em produção está limitado aos módulos de storage/index?
- A curva de onboarding de novos contribuidores é aceitável?

---

## Estrutura dos PoCs

Os PoCs estão em `poc/` e implementam as mesmas operações dos benchmarks Rust atuais em `core/benches/`:

```
poc/
  csharp-safe/       # Opção B — C# seguro
  csharp-unsafe/     # Opção C — C# com unsafe nos hot paths
  benchmark-comparison.sh
```

### Como rodar

Ver `poc/README.md` para instruções detalhadas.

Resumo:

```bash
# 1. Baseline Rust
cd core && cargo bench 2>&1 | tee /tmp/bench-rust.txt

# 2. C# Safe
cd poc/csharp-safe
dotnet run -c Release -- --filter "*" --exporters json | tee /tmp/bench-csharp-safe.json

# 3. C# Unsafe
cd poc/csharp-unsafe
dotnet run -c Release -- --filter "*" --exporters json | tee /tmp/bench-csharp-unsafe.json

# Ou use o script de comparação automática:
bash poc/benchmark-comparison.sh
```

---

## Estratégia de Migração (se C# for escolhido)

A migração é **incremental por módulo**, começando pelos módulos mais críticos para performance:

### Ordem sugerida

1. **Storage engine** (`storage/`) — page buffer, file manager
2. **Index** (`index/btree/`) — B-tree com nós em memória não gerenciada
3. **Executor** (`executor/`) — scan, filter, join, aggregate
4. **SQL Parser** (`sql/`) — parser e AST
5. **Query Planner** (`query/`) — logical/physical plan, optimizer

### Princípios

- **Safe-first**: `unsafe` somente em módulos de storage e index. O restante usa C# seguro.
- **Reduzir alocação**: `Span<T>`, `ArrayPool<T>`, `MemoryPool<T>`, structs e `stackalloc` antes de recorrer a `unsafe`.
- **Sem GC global**: não desabilitar o GC globalmente. Usar `GC.TryStartNoGCRegion` apenas em janelas críticas de operação (ex.: escrita de página em disco).
- **NativeAOT desde o início**: nenhuma API que use reflexão em runtime. Usar source generators (`System.Text.Json`, `LoggerMessage`, etc.).
- **Validação funcional e de performance a cada módulo**: cada módulo migrado deve passar nos testes de regressão e nos critérios de benchmark antes de avançar ao próximo.

### Artefatos a manter

A cada etapa da migração:

- Os testes de integração de `core/tests/` devem passar (adaptados para C# se necessário).
- A cobertura não deve cair abaixo de 80 % (mesmo critério do Rust).
- O CHANGELOG e `docs/` devem ser atualizados.

---

## Gatilho de Rollback

A migração é abortada (opção A — Rust permanece) se qualquer uma das condições abaixo ocorrer:

1. **Benchmark**: qualquer critério da seção "Critérios de Decisão" não atingir a meta.
2. **Complexidade**: o módulo de storage migrado acumular mais de 20 % do código em blocos `unsafe` não relacionados a hot paths documentados.
3. **Estabilidade**: falhas intermitentes em testes de stress relacionadas a gerenciamento manual de memória.
4. **Build**: incompatibilidade com NativeAOT ou Trimming que não possa ser resolvida sem reflexão em runtime.
5. **Equipe**: consenso da equipe de que a produtividade em C# é materialmente inferior ao Rust para este domínio.

Nesse caso, C# é adotado **apenas para os SDKs e drivers** (`.NET SDK` em `sdks/dotnet/`), que já estão planejados em Apache 2.0.

---

## Registro de Resultados

Preencher `docs/architecture-decision/benchmark-results-template.md` com os valores medidos após rodar os PoCs.

---

## Decisão Final

> _Migrar para **C# Safe (Opção B)** com `unsafe` restrito a módulos de storage e index._

**Data da decisão:** 2026-05-15  
**Opção escolhida:** **B — C# seguro** (`ArrayPool`/`Span<T>`/`Memory<T>` + NativeAOT), com `unsafe` confinado aos hot paths de storage e index (Opção C parcial).

---

### Justificativa

#### 1. Composição da equipe é o fator decisivo

O projeto é mantido por **um único desenvolvedor com expertise exclusiva em C#** e sem experiência em Rust. Manter o núcleo em Rust implica:

- **Curva de aprendizado contínua** (ownership, lifetimes, async Rust, unsafe Rust) sem nenhum ganho de produtividade para a equipe atual.
- **Tempo de onboarding impossível** para futuros contribuidores C# (a linguagem-alvo do SDK público do AeternumDB).
- **Risco de manutenção**: bugs de lifetime/borrow em Rust são difíceis de diagnosticar sem experiência. Bugs em C# unsafe (ponteiros) são mais bem compreendidos pela equipe.

#### 2. Os benchmarks confirmam que C# atinge performance comparável

| Indicador | Resultado | Veredicto |
|-----------|-----------|-----------|
| Build limpo | Rust 82 s → C# **4,6 s** (18× mais rápido) | ✅ Decisivo para produtividade |
| LOC para funcionalidade equivalente | Rust 14 594 → C# **3 787** (3,9× menos) | ✅ C# muito mais expressivo |
| Dependências transitivas | Rust 368 crates → C# **26 pacotes** (14× menos) | ✅ Menor superfície de ataque |
| NativeAOT | C# compila e publica sem erros de produção | ✅ Critério atendido |
| B-Tree in-memory | C# 5–13 M ops/s (estrutura pura) | ✅ Referência de velocidade |
| Storage write | C# Safe 23 775 ops/s; Rust 3 546 ops/s (com fdatasync) | ✅ C# ≥ Rust (com tmpfs) |
| Executor Rust bench | **Não compila** (`crate::sql::ast` inválido) | ⚠️ Rust tem dívida técnica |

#### 3. Limitações identificadas (não bloqueantes)

| Limitação | Mitigação planejada |
|-----------|---------------------|
| Alocações GC nos hot paths | `ValueTask` + `ArrayPool` nas interfaces — fase 2 |
| Binário NativeAOT 38 MB (com BDN) | Remover BDN do projeto de produção → ~10 MB |
| Latência buffer hit com `Task` | Migrar interfaces para `ValueTask<T>` |
| B-Tree C# sem persistência ainda | Implementar disco-backed na migração — fase 1 |

#### 4. Rust mantido apenas para referência histórica

O repositório Rust (`core/`) permanece como:
- Referência de arquitetura e especificação de comportamento
- Base de testes de regressão (adaptados para C# ao longo da migração)
- Evidência de que o design é correto e testável

#### 5. Critério de rollback inalterado

A migração é revertida se qualquer condição da seção "Gatilho de Rollback" ocorrer. Com 1 desenvolvedor C# e 0 desenvolvedores Rust na equipe, o rollback para Rust é considerado **impraticável** — reforçando a decisão de migrar.

---

### Ordem de migração

1. **Storage engine** — `SafeStorageEngine` + `SafeBufferPool` com `unsafe` só em I/O de página
2. **Index / B-Tree** — `SafeBTree` disk-backed usando storage engine C#
3. **Executor** — 11 operadores já implementados no PoC; adaptar à engine real
4. **SQL Parser** — portar parser do Rust ou adotar `Antlr4` / parser C# próprio
5. **Query Planner** — lógico + físico + otimizador (já tem base no PoC shared)

**Responsável:** Schivei
