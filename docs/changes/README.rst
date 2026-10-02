Changelog snippets
==================

Every pull request adds one or more small files here; ``towncrier`` assembles them into
``CHANGES.rst`` at release time and deletes them.

File names are ``<PR number>.<type>.rst``, with a counter before the extension when one
pull request has several changes of the same type: ``11.bugfix.1.rst``,
``11.bugfix.2.rst``. The types are:

``breaking``
    Something that makes existing scripts, configurations or outputs behave differently.
``feature``
    New behaviour, a new mission, a new command.
``bugfix``
    Something that was wrong and is now right.
``doc``
    Documentation only.
``maintenance``
    Tests, CI, packaging, refactoring: nothing a user of the pipeline would notice.

Write one or two plain sentences, in the present or past tense, saying what changed from
the user's point of view. The pull request number is added as a link automatically.

To get the number the next pull request will get and a ready-made file name::

    nextpr -t bugfix

(``nextpr`` is installed with ``pip install nextpr``.)

To preview the next release notes without touching anything::

    towncrier build --draft --version X.Y

To publish a release, run the same command without ``--draft``, then commit
``CHANGES.rst`` and the removed snippets.
