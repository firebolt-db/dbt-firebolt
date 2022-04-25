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

### Versioning

Consider adopting: 
 * https://packboard.atlassian.net/wiki/x/AYC6aQ
 * https://python-semantic-release.readthedocs.io/en/latest/
