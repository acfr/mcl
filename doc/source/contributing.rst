.. include:: aliases.rst

=========================
Contributing
=========================

The |MCL| code base is version controlled using Git_.

Identify Yourself
-------------------------

If you are contributing to the code base, make sure your contact details have
been correctly setup. Do not use an obscure identity in the |MCL| repositories -
this will make it difficult to associate code changes with a contributor. An
email address which identifies the author is ideal. This can be done by issuing

.. code-block:: bash

    git config user.email "<name>@<domain>.com"

in the root of the repository or system wide for all repositories

.. code-block:: bash

    git config user.email --global "<name>@<domain>.com"


Commit Workflow
-------------------------

When committing code to the |MCL| repository, good commit hygiene ensures that
collaborators can monitor changes to the code base. Some guidelines for good
development practise are:

    - Development including bug fixes and new features should be developed on a
      local branch based on 'master' (in general).

    - Make separate commits for logically separate changes. To reduce the amount
      of "patch noise", do not submit trivial or minor changes.

    - The |MCL| project favours rebase_ rather than merge_ commits on the
      ``master`` branch. A clean linear history of code changes is preferred
      over traceability of development branches. **Never** abuse the rebase
      command to modify the history of commits that have been pushed into a
      public repository (the same goes for ``git commit --amend`` and ``git
      reset``).

The following is an example of development adhering to this work-flow

#. Get latest changes using:

    .. code-block:: bash

        git checkout master
        git fetch origin
        git merge master

    or

    .. code-block:: bash

        git checkout master
        git pull origin master

#. Isolate development of feature (or bug fix) on a new branch:

    .. code-block:: bash

        git checkout -b newfeature

#. Work on feature
#. Import commits from the remote repository and integrate with the feature
   branch:

    .. code-block:: bash

        git fetch origin
        git rebase origin/master

#. Repeat steps 3 & 4 until development has completed. Be sure to finalise
   development by completing step 4 before attempting to integrate your changes
   with the remote repository.

#. Integrate changes with the remote repository:

    .. code-block:: bash

        git checkout master
        git pull
        git rebase newfeature

    To interactively edit the sequence of commits on the feature branch which
    are about to be rebased use:

    .. code-block:: bash

        git rebase -i newfeature

    Finally,

    .. code-block:: bash

        git push


Commit Logs
-------------------------

Good commit hygiene also includes how commit messages are create. A good commit
message will effectively communicate your changes to collaborators. Concise yet
descriptive messages are important when viewing the commit history. Good commit
messages contain:

#. A short description (soft limit of 50 characters) of the commit on the first
   line.

    - The first line should not be terminated by a full stop.
    - The first line is often prefixed with the main location of
      development. For example::

          network: read multiple items from IPv6, UDP socket

#. A blank line
#. A more detailed description of the commit wrapped to 72 characters.

An example of a good commit message is included from the `git documentation
<http://git-scm.com/book/en/Distributed-Git-Contributing-to-a-Project>`_::


    Short (50 chars or less) summary of changes

    More detailed explanatory text, if necessary.  Wrap it to about 72
    characters or so.  In some contexts, the first line is treated as the
    subject of an email and the rest of the text as the body.  The blank
    line separating the summary from the body is critical (unless you omit
    the body entirely); tools like rebase can get confused if you run the
    two together.

    Further paragraphs come after blank lines.

     - Bullet points are okay, too

     - Typically a hyphen or asterisk is used for the bullet, preceded by a
       single space, with blank lines in between, but conventions vary here
