### Development Setup

1. Clone this repo into a Python `3.9` virtual environment.
1. `pip install -e ".[dev]"`. Ignore the warning if there's no dev option in setup.cfg.
1. `pre-commit install`
1. If using PyCharm, set the test runner as `pytest` and mark `src` as a sources root.

Optionally setup PyCharm linting shortcut:

1. Preferences -> Tools -> External Tools > + (add new entry)
```
Name: lint
Description: Format the current file
Program: $PyInterpreterDirectory$/pre-commit
Arguments: run --files=$FilePath$Working 
Working Directory: $ProjectFileDir$
```
2. Preferences -> Keymap -> External Tools -> lint, 
   Assign the keyboard shortcut `Option-cmd-l`

### Before Committing

1. The pre-commit hook should catch linting errors
2. run `pytest tests/unit` to run unit tests

### PR procedures

 1. When a pull request is created a set of automated tests are run. If any of those fail they will need to be fixed before any of the maintainers can review the PR. The checks here include:
 - Unit tests
 - Linting
 - Type checks
 - PR title correctness

2. If all automated tests have succeeded, the change is reviewed by one of the maintainers, assesing the need for the change and adding suggestions.
3. Maintainer kicks off integration tests. Those can only be submitted by the maintainers in order to avoid an abuse of resources.
4. If the integration tests pass and the change looks good to the maintainer they approve it.
5. Merge into the main branch. Only the maintainers have the ability to merge a PR. They will do so at the earliest convenience, with regards to the impact of the change as well as the release planning.

### Docstrings

Use the Google format for docstrings. Do not include types or an indication 
of "optional" in docstrings. Those should be captured in the function signature 
as type annotations; no need to repeat them in the docstring.

Public methods and functions should have docstrings. 
One-liners are fine for simple methods and functions.

For PyCharm Users:

1. Tools > Python Integrated Tools > Docstring Format: "Google"
2. Editor > General > Smart Keys > Check "Insert documentation comment stub"
3. Editor > General > Smart Keys > Python > Uncheck "Insert type placeholders..."

### Import style

In general, prefer `from typing import Optional, ...`, and not `import typing`.

### Method Order

In general, organize class internals in this order:

1. class attributes
2. `__init__()`
3. classmethods (`@classmethod`)
   * alternative constructors first
   * other classmethods next
4. properties (`@property`)
5. remaining methods 
   * put more important / broadly applicable functions first
   * group related functions together to minimize scrolling

Read more about this philosophy 
[here](https://softwareengineering.stackexchange.com/a/199317).

### Huge classes

If classes start to approach 1k lines, consider breaking them into parts, 
possibly like [this](https://stackoverflow.com/a/47562412).

### Generating Models

If you have a json response from the API, you can try running the following:
```python
engine_revision = {"something": "parsed_from_api"}

import pathlib
import json
with pathlib.Path("./engine_revision.json").open("w") as f:
    f.write(json.dumps(engine_revision))
```

From this file, you can [generate models](
https://pydantic-docs.helpmanual.io/datamodel_code_generator/), like this:
```shell
pip install datamodel-code-generator
datamodel-codegen \
   --input-file-type json \
   --input engine_revision.json \
   --output engine_revision.py
```

You can also generate code from a json string in python:
```python
from datamodel_code_generator import generate, InputFileType
import json
instance = {'name': 'r5.8xlarge'}
instance_json = json.dumps(instance)

# calling generate will print out the generated code
generate(
    input_=instance_json,
    input_file_type=InputFileType.Json
)
```

Or, you can try generating from the json api [spec](
https://api.app.firebolt.io/docs/openapi.json):
```shell
datamodel-codegen \
  --url https://api.app.firebolt.io/docs/openapi.json \
  --output out/openapi.py
```

Note: this did not work for me, I suspect we may have to [dereference](
https://github.com/koxudaxi/datamodel-code-generator/issues/500) first.
But it is a feature I would like to use in the future.

### Versioning

Consider adopting: 
 * https://packboard.atlassian.net/wiki/x/AYC6aQ
 * https://python-semantic-release.readthedocs.io/en/latest/
