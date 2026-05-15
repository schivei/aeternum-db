# AeternumDB — PoC de Avaliação de Linguagem

Este diretório contém as provas de conceito (PoCs) para avaliar a migração do núcleo do AeternumDB de Rust para C#, conforme descrito em [`docs/architecture-decision/ADR-001-language-migration-evaluation.md`](../docs/architecture-decision/ADR-001-language-migration-evaluation.md).

---

## Estrutura

```
poc/
  csharp-safe/          # PoC C# com código seguro (ArrayPool, Span<T>, structs)
  csharp-unsafe/        # PoC C# com unsafe nos hot paths (NativeMemory, ponteiros)
  benchmark-comparison.sh   # Script que roda Rust + ambos C# e exibe comparação
```

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|------------|--------------|
| Rust + Cargo | 1.75+ |
| .NET SDK | 9.0+ |
| `cargo-criterion` (opcional) | qualquer |

---

## Operações Benchmarkadas

Os PoCs medem exatamente as mesmas operações presentes em `core/benches/`:

| Módulo | Operações |
|--------|-----------|
| **Storage** | Escrita sequencial, leitura aleatória, misto 80r/20w, buffer hit |
| **B-Tree** | Inserção sequencial, inserção aleatória, point query, range scan, delete, bulk load |
| **Row Scan** | Seq scan sem filtro, seq scan com filtro, VALUES executor |

---

## Como Rodar

### Opção 1 — Script automático (recomendado)

```bash
cd poc
bash benchmark-comparison.sh
```

O script gera um arquivo `poc/results/comparison-<timestamp>.md` com os resultados comparativos.

### Opção 2 — Rodar individualmente

#### Baseline Rust

```bash
cd core
cargo bench 2>&1 | tee /tmp/bench-rust.txt
```

#### C# Safe

```bash
cd poc/csharp-safe
dotnet run -c Release -- --filter "*" --exporters json markdown
```

#### C# Unsafe

```bash
cd poc/csharp-unsafe
dotnet run -c Release -- --filter "*" --exporters json markdown
```

---

## NativeAOT / Trimming

Para verificar compatibilidade NativeAOT:

```bash
# C# Safe
cd poc/csharp-safe
dotnet publish -r linux-x64 -c Release /p:PublishAot=true

# C# Unsafe
cd poc/csharp-unsafe
dotnet publish -r linux-x64 -c Release /p:PublishAot=true
```

Nenhum aviso de trim deve aparecer. O binário gerado em `publish/` deve ser executável sem .NET runtime instalado.

---

## Registrar Resultados

Preencher [`docs/architecture-decision/benchmark-results-template.md`](../docs/architecture-decision/benchmark-results-template.md) com os valores medidos e submeter PR com os resultados.
