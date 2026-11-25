"""Константи шару Application, спільні для обробників і процесів."""
from uuid import UUID

RATE_KEY = "config:rate"  # ключ Redis із цільовою швидкістю генератора (док/с)

# Простір імен для UUID5 препаратів. Має збігатися з NAMESPACE у scripts/process_dataset.py,
# тоді препарат, створений через API, отримає той самий id, що й у датасеті.
MEDICINE_NAMESPACE = UUID("12345678-1234-5678-1234-567812345678")
