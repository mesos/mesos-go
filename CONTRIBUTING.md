# Contributing

## Issues

When filing an issue, make sure to answer these five questions:

1. What version of the project are you using?
2. What operating system and processor architecture are you using?
3. What did you do?
4. What did you expect to see?
5. What did you see instead?

## Design

This project's development process is design-driven. Apart from simple
bug fixes, typo corrections and the like, significant changes must be
first discussed, and sometimes formally documented, before they can be implemented.

This document describes the process for proposing, documenting, and implementing
such changes. It's forked and modified from the Go language proposal process.

### Goals

- Make sure that proposals get a proper, fair, timely, recorded evaluation with
  a clear answer.
- Make past proposals easy to find, to avoid duplicated effort.
- If a design doc is needed, make sure contributors know how to write a good one.

### Definitions

- A **proposal** is a suggestion filed as a GitHub issue, identified by having
  the "Proposal:" title prefix.
- A **design doc** is the expanded form of a proposal, written when the
  proposal needs more careful explanation and consideration.

### Scope

The proposal process should be used for any notable change or addition.
Since proposals begin (and will often end) with the filing of an issue, even
small changes can go through the proposal process if appropriate.
Deciding what is appropriate is matter of judgment we will refine through
experience.
If in doubt, file a proposal.

### Process

- Create an issue describing the proposal.

- Like any GitHub issue, a Proposal issue is followed by an initial discussion
  about the suggestion. For Proposal issues:
  - The goal of the initial discussion is to reach agreement on the next step:
    (1) accept, (2) decline, or (3) ask for a design doc.
  - The discussion is expected to be resolved in a timely manner.
  - If the author wants to write a design doc, then they can write one.
  - A lack of agreement means the author should write a design doc.
  - If there is disagreement about whether there is agreement,
    the project's main commiter is the arbiter.

- If a Proposal issue leads to a design doc:
  - The design doc should be checked in to `design/NNNN-shortname.md`,
    where `NNNN` is the GitHub issue number and `shortname` is a short name
    (a few dash-separated words at most).
  - The design doc should follow the template found below.
  - The design doc should address any specific issues asked for during the
    initial discussion.
  - It is expected that the design doc may go through multiple checked-in revisions.
  - New design doc authors may be paired with a design doc "shepherd" to help work
    on the doc.
  - Comments by others can be made on the GitHub issue.

- Once comments and revisions on the design doc wind down, there is a final
  discussion about the proposal.
  - The goal of the final discussion is to reach agreement on the next step:
    (1) accept or (2) decline.
  - The discussion is expected to be resolved in a timely manner.
  - A lack of agreement means decline.
  - If there is disagreement about whether there is agreement, the
    project's main commiter is the arbiter.

- The author (and/or other contributors) do the work as described by the
  "Implementation" section of the proposal.

### Template
```markdown
# Proposal: [Title]

Author(s): [Author Name, Co-Author Name]

Last updated: [Date]

## Abstract

[A short summary of the proposal.]

## Background

[An introduction of the necessary background and the problem being solved by the proposed change.]

## Proposal

[A precise statement of the proposed change.]

## Rationale

[A discussion of alternate approaches and the trade offs, advantages, and disadvantages of the specified approach.]

## Implementation

[A description of the steps in the implementation, who will do them, and when.]

## Open issues (if applicable)

[A discussion of issues relating to this proposal for which the author does not
know the solution. This section may be omitted if there are none.]
```

## Implementation

Every contribution must conform to the following rules:

- Changed or added code must be well tested. Different kinds of code
  require different testing strategies.
- Changed or added code must pass the project's CI.
- Work should happen in an open pull request having a WIP prefix in its title.
- The pull request description must be well written and provide every
  bit of necessary context and background. If there's a design doc, it
  must be linked.
- Once ready for review, replace the WIP prefix with PTAL which will
  bring your contribution to the attention of project maintainers.
- One or preferably two project commiters will review your PR in a
  timely manner.
- Once comments and revisions on the implementation wind down, one or
  preferably two reviewers must add LGTM comments to the pull request.
  There must be consensus across reviewers before merging.
- Upon merge, all git commits must represent meaningful milestones or units
  of work. Use commits to add clarity to the development and review process.
  In addition, changes to vendored files must be grouped into a single commit.
