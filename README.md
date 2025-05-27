[![codecov](https://codecov.io/gh/BioPack-team/shepherd/graph/badge.svg?token=NTPV9WF7EO)](https://codecov.io/gh/BioPack-team/shepherd)

# Translator Shepherd Service

Shepherd is a shared platform for ARA implementation. Incorporated ARAs have access to a plethora of shared ARA functionality while retaining the ability to implement their own custom operations.

## Local Development

All Shepherd services are set up as docker containers. You can learn about docker and install here: https://www.docker.com/

The main entrypoint is `./compose.yml` and will spin everything up.

- In the root folder, run `docker compose up --build`

If you want to add a new operation/worker, add a new service in `compose.yml` under `services`.

### Worker

Each worker is it's own separate docker container. It spins up and begins to watch a central message broker for tasks to work on. Once it gets a task, it
can do that task either synchronously, asynchronously, on a separate process etc. based on its individual resource requirements.

Each worker has access to a shared utilities library that aids in db and message broker interaction as well as other functions that are common across ARAs. Check the
shared function library before writing a new function that you think other ARAs might also want to use.

### Message Broker Streams

Shepherd uses Redis Streams for its message broker. More info on Redis Streams can be found [here](https://redis.io/docs/latest/develop/data-types/streams/)
Each worker type listens to its own message stream/queue. The shared workers can just use their Translator workflow name for their stream name. For workers that share the same workflow but are for different ARAs need their own custom stream name. The current convention is to use the format `{ara_name}.{workflow_name}`.
Multiple workers of the same type can be in the same `GROUP`, and redis will make sure not to give out the same task to more than one worker in that group. The current convention is to call the group `consumer`.

### Creating your own ARA

Creating a brand new ARA is fairly straightforward. Here are the steps to create a basic ARA that performs all the necessary Translator operations:
- Copy the `example_ara` folder. This will be the main entrypoint to your new ARA.
  - Towards the top of that file, replace the `STREAM` variable value with your ARA name.
  - Within the `example_ara` function in that file, replace the `workflow` list with your ARA's workflow. This could include analyzing the `message` to determine a pertinent workflow.
  - **Note:** If using operations not in the shared workers, your workflow operation ids need to reflect the `STREAM` name of your custom operation workers. This is how the task get passed to your operation worker.
- (If needed) Copy the `example_lookup` folder. This will be your ARA's lookup operation.
  - Towards the top of that file, replace the `STREAM` variable value with `{ara_name}.lookup`.
  - Within the `example_lookup` function in that file, replace the contents with your ARA's lookup logic.
- (If needed) Copy the `example_score` folder. This will be your ARA's score operation.
  - Towards the top of that file, replace the `STREAM` variable value with `{ara_name}.score`.
  - Within the `example_score` funciton in that file, replace the contents with your ARA's scoring logic.
- If you have other custom operations you want to perform that are in your `workflow` above, pick a similar folder to copy and adjust the code inside to fit your needs.
- If you want to use shared workers (i.e. `sort_results_score`, `filter_results_top_n`), you don't need to do anything other than include them in your workflow. They will automatically pick up your query and pass it along like your other operations.
- Open the `compose.yml` file in the root directory, and for each ARA folder you created, add a `service` (or copy an existing one), and make sure that the `container_name` and `build/dockerfile` reflect you worker names and the path to your worker Dockerfile
- Run Shepherd with `docker compose up --build`

### Testing your ARA

TODO: we're going to make a bunch of unit tests for each type of worker.
Run the `scripts/test_shepherd.py` script to run a query against your ARA. Replace the `target` argument with your ARA name so the server routes the query to your worker. This script requires that Shepherd be running locally.