# shepherd
A Translator ARA that aims to be modular in its query expansion and ranking, among other TRAPI workflow operations.

## Architecture
Shepherd's tech stack consists of a FastAPI server that handles all web traffic with multiple workers, async python so it is never stuck waiting on a web request, and PostgreSQL that acts as the central state management.

### Typical Workflow
- Shepherd receives a TRAPI query from the ARS.
- It contacts all of its query expansion modules and creates all of the queries.
- It sends all of the individual queries to the Retriever.
- Once it receives those queries back, it does any message merging across the different expanded queries.
- It then sends the merged message on to the other specified workflow operations, i.e. different scoring modules.

## Development
You can run everything through Docker, just uncomment the `shepherd` section in the `compose.yml` file.
Run the postgres through Docker:
- docker compose up
If you want to run Shepherd through local python:
_It is recommended that you use a python virtual environment_
- python -m venv <path_to_venv>
- source <path_to_venv>/bin/activate
Install dependencies
- pip install -r requirements.txt
Install Shepherd
- pip install -e .
Start
- python main.py
