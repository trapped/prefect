from prefect.runtimes.flow_cli import FlowCLI
from prefect.runtimes.make_cli import MakeCLI

# Motivation for runtimes:

# "Runtimes" allow for behavior that wraps core functionality.
# This is important as it allows us to pluggably bring opinionated ways of using core without changing core.
# Examples of this would be to streamline code expression and restrict unused flexibility to allow for "a single way" to do an action.
