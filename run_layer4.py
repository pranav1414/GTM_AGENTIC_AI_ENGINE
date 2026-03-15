import sys
sys.path.insert(0, ".")

import importlib.util

for mod in ["tools", "output", "agents", "orchestrator"]:
    spec = importlib.util.spec_from_file_location(
        f"layer4.{mod}", f"Agentic Layer/{mod}.py"
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules[f"layer4.{mod}"] = m
    spec.loader.exec_module(m)

from layer4.orchestrator import run_layer4
run_layer4(min_score=0)