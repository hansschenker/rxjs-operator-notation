# Operator analysis: OPERATOR_NAME

Copy this file when adding an operator card. Replace the prompts with concrete statements and remove unused example notation. Use [Foundation v0.1](../docs/RxJS-Operator-Behavior-Notation-Foundation-v0.1.md) for symbol definitions and assumptions.

## Contract

| Field | Specify |
| --- | --- |
| RxJS version | 7.8.2 |
| Operator form | Exact overload and arguments |
| Type sketch | Input and output value types |
| Purpose | The practical behavior this operator provides |
| Supplied functions | Named functions, their types, and their assumptions |
| Source kinds | Cold, hot, or explicitly shared; synchronous or asynchronous behavior |
| Scheduler | Scheduler and clock, or no scheduling introduced |
| Scope | What this card describes; variants requiring another card |

## What flows over time

Describe the input notifications and the output value shape. Define input labels and subscription identities. State whether output values retain references or represent newly constructed containers.

## What happens on a new value

Write the smallest useful next-notification rule:

$$
\{t,N(v)\}_s\longrightarrow\text{output or actions}
$$

If a previous value, counter, queue, or other memory affects that rule, include the state:

$$
(\sigma,\{t,N(v)\}_s)
\longrightarrow
(\sigma',[\text{ordered actions}])
$$

Explain any condition producing $\varnothing$. Distinguish no output from an emitted `undefined`, completion, and an empty inner source.

## State and ownership

| State item | Initial value | Owner | Update rule | Reset or disposal |
| --- | --- | --- | --- | --- |
| Fill in | Fill in | Result subscription, shared connection, or other explicit owner | Fill in | Fill in |

Include input indices if user functions depend on them. Distinguish missing values from valid `undefined` or `null` values.

## Triggers and ordered reactions

| Trigger | State change | Ordered actions |
| --- | --- | --- |
| Downstream subscribes | Specify | Specify source and notifier subscription order |
| Source next | Specify | Specify |
| Source complete | Specify | Specify |
| Source error | Specify | Specify |
| Supplied function throws | Specify | Specify |
| Inner or notifier next | Specify if applicable | Specify |
| Inner or notifier complete | Specify if applicable | Specify |
| Inner or notifier error | Specify if applicable | Specify |
| Timer fires | Specify if applicable | Specify |
| Downstream unsubscribes | Specify | Specify teardown and pending-work cancellation |

Delete genuinely inapplicable rows. Do not infer notifier completion behavior from its next behavior.

## Time and event order

State which event determines output time. If work is scheduled, give its due-time rule and task identity. Describe equal-time ordering when it changes results. If nested synchronous execution matters, split the rule into phases rather than assuming an atomic reaction.

## Cancellation and sharing

State which subscriptions end on cancellation and which producers may remain active for other consumers. Define the sharing boundary, subscriber membership, connection lifetime, and reset policy if sharing is involved. Otherwise state that the operator introduces no sharing while preserving any sharing already present in its inputs.

## Worked trace

Describe the initial state and subscription start times before the table. Use an example that distinguishes this operator's policy from related policies.

| Time and order | Incoming event | State afterward | Output or action |
| --- | --- | --- | --- |
| Fill in | Fill in | Fill in | Fill in |

Include a separate subscription-interval table when connection lifetimes are part of the behavior.

## Boundary cases to resolve

Choose the cases needed for this operator:

- Empty, single-value, never-ending, or erroring input.
- Completion with pending values, timers, buffers, or inner subscriptions.
- Cancellation before scheduled delivery or inner completion.
- Synchronous input and synchronous inner or notifier sources.
- Several events at the same logical time.
- Missing first values and valid `undefined` values.
- Exceptions or index dependence in supplied functions.
- Repeated subscriptions and state isolation.
- Late subscribers and reset after the last subscriber leaves.
- Reentrant notifications or subscription changes, if within the card's scope.

## Evidence

| Claim | Evidence | Status |
| --- | --- | --- |
| Main trace | Link to a test against RxJS 7.8.2 | Executed or pending |
| Subscription lifetime | Link to subscription assertions | Executed or pending |
| Boundary behavior | Source link and/or a discriminating test | Executed, inspected, or unresolved |

Separate model assumptions, implementation-derived behavior, and executed observations. Do not describe a behavior as tested merely because a nearby example passed.

## Remaining limitations

List unsupported overloads, unmodeled effects, or ordering cases that need a more detailed description. State what would need to be added to the model for those cases.

## Sources

Link to the pinned 7.8.2 source and relevant official documentation. Where prose documentation and the pinned implementation disagree, preserve the discrepancy and show which behavior was verified.
