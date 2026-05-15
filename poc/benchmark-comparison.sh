#!/usr/bin/env bash
# benchmark-comparison.sh
# Roda os benchmarks Rust (baseline) e ambos os PoCs C# e gera um relatório
# comparativo em poc/results/comparison-<timestamp>.md
#
# Uso: bash poc/benchmark-comparison.sh [--skip-rust] [--skip-safe] [--skip-unsafe]
#
# Pré-requisitos: Rust/Cargo, .NET 9+ SDK

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
POC_DIR="$REPO_ROOT/poc"
CORE_DIR="$REPO_ROOT/core"
RESULTS_DIR="$POC_DIR/results"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
REPORT="$RESULTS_DIR/comparison-$TIMESTAMP.md"

SKIP_RUST=0
SKIP_SAFE=0
SKIP_UNSAFE=0

for arg in "$@"; do
  case "$arg" in
    --skip-rust)   SKIP_RUST=1 ;;
    --skip-safe)   SKIP_SAFE=1 ;;
    --skip-unsafe) SKIP_UNSAFE=1 ;;
  esac
done

mkdir -p "$RESULTS_DIR"

echo "# AeternumDB Benchmark Comparison" > "$REPORT"
echo "" >> "$REPORT"
echo "**Gerado em:** $TIMESTAMP" >> "$REPORT"
echo "**Host:** $(uname -srm)" >> "$REPORT"
if command -v rustc &>/dev/null; then
  echo "**Rust:** $(rustc --version)" >> "$REPORT"
fi
if command -v dotnet &>/dev/null; then
  echo "**.NET:** $(dotnet --version)" >> "$REPORT"
fi
echo "" >> "$REPORT"

# ── 1. Rust baseline ──────────────────────────────────────────────────────────
RUST_OUTPUT="$RESULTS_DIR/rust-$TIMESTAMP.txt"

if [ "$SKIP_RUST" -eq 0 ]; then
  echo "==> Rodando benchmarks Rust..."
  cd "$CORE_DIR"
  cargo bench 2>&1 | tee "$RUST_OUTPUT"
  echo "## Resultados Rust (baseline)" >> "$REPORT"
  echo "" >> "$REPORT"
  echo '```' >> "$REPORT"
  grep -E "(test |bench )" "$RUST_OUTPUT" | head -60 >> "$REPORT" || true
  echo '```' >> "$REPORT"
  echo "" >> "$REPORT"
  echo "Arquivo completo: \`$RUST_OUTPUT\`" >> "$REPORT"
  echo "" >> "$REPORT"
  echo "  OK"
else
  echo "==> Pulando benchmarks Rust (--skip-rust)"
  echo "## Resultados Rust — pulado (--skip-rust)" >> "$REPORT"
  echo "" >> "$REPORT"
fi

# ── 2. C# Safe ────────────────────────────────────────────────────────────────
SAFE_DIR="$POC_DIR/csharp-safe"
SAFE_OUTPUT="$RESULTS_DIR/csharp-safe-$TIMESTAMP"

if [ "$SKIP_SAFE" -eq 0 ]; then
  echo "==> Rodando benchmarks C# Safe..."
  cd "$SAFE_DIR"
  dotnet run -c Release -- \
    --filter "*" \
    --exporters json markdown \
    --artifacts "$SAFE_OUTPUT" \
    2>&1 | tee "$SAFE_OUTPUT.txt"
  echo "## Resultados C# Safe" >> "$REPORT"
  echo "" >> "$REPORT"
  if [ -f "$SAFE_OUTPUT/results/AeternumDB.PoC.Safe.Benchmarks-report-github.md" ]; then
    cat "$SAFE_OUTPUT/results/AeternumDB.PoC.Safe.Benchmarks-report-github.md" >> "$REPORT"
  else
    echo '```' >> "$REPORT"
    tail -40 "$SAFE_OUTPUT.txt" >> "$REPORT"
    echo '```' >> "$REPORT"
  fi
  echo "" >> "$REPORT"
  echo "  OK"
else
  echo "==> Pulando benchmarks C# Safe (--skip-safe)"
  echo "## Resultados C# Safe — pulado (--skip-safe)" >> "$REPORT"
  echo "" >> "$REPORT"
fi

# ── 3. C# Unsafe ─────────────────────────────────────────────────────────────
UNSAFE_DIR="$POC_DIR/csharp-unsafe"
UNSAFE_OUTPUT="$RESULTS_DIR/csharp-unsafe-$TIMESTAMP"

if [ "$SKIP_UNSAFE" -eq 0 ]; then
  echo "==> Rodando benchmarks C# Unsafe..."
  cd "$UNSAFE_DIR"
  dotnet run -c Release -- \
    --filter "*" \
    --exporters json markdown \
    --artifacts "$UNSAFE_OUTPUT" \
    2>&1 | tee "$UNSAFE_OUTPUT.txt"
  echo "## Resultados C# Unsafe" >> "$REPORT"
  echo "" >> "$REPORT"
  if [ -f "$UNSAFE_OUTPUT/results/AeternumDB.PoC.Unsafe.Benchmarks-report-github.md" ]; then
    cat "$UNSAFE_OUTPUT/results/AeternumDB.PoC.Unsafe.Benchmarks-report-github.md" >> "$REPORT"
  else
    echo '```' >> "$REPORT"
    tail -40 "$UNSAFE_OUTPUT.txt" >> "$REPORT"
    echo '```' >> "$REPORT"
  fi
  echo "" >> "$REPORT"
  echo "  OK"
else
  echo "==> Pulando benchmarks C# Unsafe (--skip-unsafe)"
  echo "## Resultados C# Unsafe — pulado (--skip-unsafe)" >> "$REPORT"
  echo "" >> "$REPORT"
fi

# ── 4. Sumário ────────────────────────────────────────────────────────────────
echo "## Próximos Passos" >> "$REPORT"
echo "" >> "$REPORT"
echo "1. Copie os números para \`docs/architecture-decision/benchmark-results-template.md\`." >> "$REPORT"
echo "2. Calcule a razão C#/Rust para cada operação." >> "$REPORT"
echo "3. Compare com as metas em \`ADR-001-language-migration-evaluation.md\`." >> "$REPORT"
echo "4. Registre a decisão final no ADR." >> "$REPORT"
echo "" >> "$REPORT"

echo ""
echo "==> Relatório gerado: $REPORT"
