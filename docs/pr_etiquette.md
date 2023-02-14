Pull Request Etiquette
=======

*this is copied and adapted from https://gist.github.com/mikepea/863f63d6e37281e329f8*

### Why do we use a Pull Request workflow?

PRs are a great way of sharing information, and can help us be aware of the
changes that are occuring in our codebase. They are also an excellent way of
getting peer review on the work that we do, without the cost of working in
direct pairs.

**Ultimately though, the primary reason we use PRs is to encourage quality in
the commits that are made to our code repositories**

Done well, the commits (and their attached messages) contained within tell a
story to people examining the code at a later date. If we are not careful to
ensure the quality of these commits, we silently lose this ability.

**Poor quality code can be refactored. A terrible commit lasts forever.**


### What constitutes a good PR?

A good quality PR will have the following characteristics:

* It will be a complete piece of work that adds value in some way.
* It will have a title that reflects the work within, and a summary that helps to understand the context of the change.
* There will be well written commit messages, with well crafted commits that tell the story of the development of this work.
* Ideally it will be small and easy to understand. Single commit PRs are usually easy to submit, review, and merge.
* The code contained within will meet the best practises set by the team wherever possible.

A PR does not end at submission though. A code change is not made until it is merged and used in production.

A good PR should be able to flow through a peer review system easily and quickly.

Submitting Pull Requests
----

### Ensure there is a solid title and summary

PRs are a Github workflow tool, so it's important to understand that the PR
title, summary and eventual discussion are not as trackable as the the commit
history. If we ever move away from Github, we'll likely lose this infomation.

That said however, they are a very useful aid in ensuring that PRs are handled
quickly and effectively.

Ensure that your PR title is scannable. People will read through the list of
PRs attached to a repo, and must be able to distinguish between them based on
title. Include a story/issue reference if possible, so the reviewer can get any
extra context. Include a reference to the subsystem affected, if this is a
large codebase.


### Use the Draft feature and proper labels

If your PR is not fully ready yet for reviews, convert it to a `draft` so people
don't waste time reviewing unfinished code.

Use the proper lables to help people understand your intention with the PR and 
its scope.


### Keep your branch up-to-date

Unless there is a good reason not to rebase - typically because more than one
person has been working on the branch - it is often a good idea to rebase your
branch with the latest `main` to make reviews easier.

### Keep it small

Try to only fix one issue or add one feature within the pull request. The
larger it is, the more complex it is to review and the more likely it will be
delayed. Remember that reviewing PRs is taking time from someone else's day.

If you must submit a large PR, try to at least make someone else aware of this
fact, and arrange for their time to review and get the PR merged. It's not fair
to the team to dump large pieces of work on their laps without warning.

If you can rebase up a large PR into multiple smaller PRs, then do so.


Reviewing Pull Requests
-----

It's a reviewers responsibility to ensure:

* Commit history is excellent
* Good changes are propagated quickly
* Code review is performed
* They understand what is being changed, from the perspective of someone examining the code in the future.

### Keep the flow going

Pull Requests are the fundamental unit of how we progress change. If PRs are
getting clogged up in the system, either unreviewed or unmanaged, they are
preventing a piece of work from being completed.

As PRs clog up in the system, merges become more difficult, as other features
and fixes are applied to the same codebase. This in turn slows them down
further, and often completely blocks progress on a given codebase.

There is a balance between flow and ensuring the quality of our PRs. As a
reviewer you should make a call as to whether a code quality issue is
sufficient enough to block the PR whilst the code is improved. Possibly it is
more prudent to simply flag that the code needs rework, and raise an issue.

Any quality issue that will obviously result in a bug should be fixed.

### We are all reviewers

To make sure PRs flow through the system speedily, we must scale the PR review
process. It is not sufficient (or fair!) to expect one or two people to review
all PRs to our code. For starters, it creates a blocker every time those people
are busy.

Hopefully with the above guidelines, we can all start sharing the responsibility of being a reviewer.

NB: With this in mind - if you are the first to comment on a PR, you are that
PRs reviewer. If you feel that you can no longer be responsible for the
subsequent merge or closure of the PR, then flag this up in the PR
conversation, so someone else can take up the role.

There's no reason why multiple people cannot comment on a PR and review it, and
this is to be encouraged.


### Don't add to the PR yourself.

It's sometimes tempting to fix a bug in a PR yourself, or to rework a section
to meet coding standards, or just to make a feature better fit your needs.

If you do this, you are no longer the reviewer of the PR. You are a
collaborator, and so should not merge the PR.

It is of course possible to find a new reviewer, but generally change will be
speedier if you require the original submitter to fix the code themselves.
Alternatively, if the original PR is 'good enough', raise the changes you'd
like to see as separate stories/issues, and rework in your own PR.

### It is not the reviewers responsibility to test the code

We are all busy people, and in the case of many PRs against our codebase we are
not able or time-permitted to test the new code.

We need to assume that the submitter has tested their code to the point of
being happy with the work to be merged to master and subsequently released.

If you, as a reviewer, are suspicious that the work in the PR has not been
tested, raise this with the submitter. Find out how they have tested it, and
refuse the work if they have not. They may not have a mechanism to test it, in
which case you may need to help.

If, as a submitter, you know that this change is not fully tested, highlight
this in the PR text, and talk to the reviewer.

One exception is when the PR needs a review on a specific architecture.


