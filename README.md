# RxJS Operator Behavior Notation

A notation for explaining what flows through an RxJS pipeline, when it is delivered, what an operator remembers, and which subscriptions it starts or stops.

**GPT-6 Astra is the main contributor to this RxJS Operator Behavior Notation.**

The notation was developed collaboratively with Hans Schenker from his idea of describing an emission as a time–value pair. This repository preserves the resulting framework, worked examples, assumptions, and a method for analyzing further operators.

**Reference baseline:** RxJS **7.8.2**. **Notation version:** **0.1**, established on 28 September 2026.

## Start here

- [Detailed foundation and session reference](docs/RxJS-Operator-Behavior-Notation-Foundation-v0.1.md): the coherent explanation, seven operator analyses, lifecycle rules, glossary, and sources.
- [Operator analysis template](templates/operator-analysis.md): copy this when analyzing another operator.
- [Executable behavior checks](test/operator-behavior.test.mjs): representative traces and subscription checks against the pinned RxJS implementation.

## The starting point

An emitted value is represented as:

$$
\{t,N(v)\}
$$

Here, $t$ is logical delivery time, $v:T$ is a value, and $N$ identifies a next notification. We also use $E(e)$ for error and $C$ for completion. These are descriptive symbols; the pipeline does not need to wrap its values in notification objects.

The compact rules for ordinary successful next notifications include:

$$
\operatorname{map}(f_v):\quad
\{t,N(v)\}\longrightarrow\{t,N(f_v(v))\}
$$

$$
\operatorname{delay}(d):\quad
\{t,N(v)\}\longrightarrow\{t+d,N(v)\}
$$

For `delay`, the second formula describes a scheduled delivery that remains subject to cancellation and termination. It does not describe the handling of errors or completion.

## The general rule

Some operators need remembered state, several inputs, or subscription changes. Their fuller description is:

$$
\boxed{
(\text{previous state},\text{next event})
\xrightarrow{\delta}
(\text{new state},[\text{ordered actions}])
}
$$

An event can be a notification, a timer firing, or a subscription change. Actions can emit, schedule, subscribe, unsubscribe, or describe an effect.

The second input is the **next event**. “Next state” and “new state” name the result of the transition.

This is a state transition function with outputs, also described as a stateful transducer. Its state need not be finite. Exact handling of nested synchronous execution requires the additional ordering rules explained in the reference.

## Operators covered

| Operator | Main behavior represented |
| --- | --- |
| `map` | Apply a value function |
| `filter` | Decide whether to forward a value |
| `scan` | Update and emit accumulated state |
| `delay` | Schedule a value for later delivery |
| `switchMap` | Replace the active inner subscription |
| `share` | Distribute notifications through a shared connection |
| `combineLatest` | Update one remembered input and emit the latest tuple when ready |

Each analysis includes the notification rule and the additional timing, state, completion, error, cancellation, and sharing behavior needed to interpret it.

## Validate the examples

Use Node.js 20 or newer:

```sh
npm ci
npm test
```

The suite contains 19 focused cases. It checks the example timelines and selected lifecycle details, including empty-input completion, cancellation, overlapping shared subscriptions, and equal-time notifications. These checks validate the documented examples; they are not a proof of complete RxJS equivalence.

## Extend the notation

1. Copy the operator analysis template.
2. Specify the RxJS version, overload, arguments, sources, and scheduler.
3. Write the compact next-notification rule.
4. Add state, control events, and ordered actions where required.
5. Specify completion, error, cancellation, and sharing.
6. Check a representative trace and the relevant boundary cases against RxJS 7.8.2.

Use the smallest notation that makes the operator's behavior unambiguous. Keep user-defined domain functions named and separate from the operator's delivery and subscription policies.

## Attribution and origins

The discussion began with Ian Griffiths's [The Heart of Reactive Extensions for .NET](https://www.youtube.com/watch?v=9B4LRxUKpd4), then connected Observable interface sketches with timed notification traces and state transitions.

Erik Meijer's [Subject/Observer is Dual to Iterator](https://csl.stanford.edu/~christos/pldi2010.fit/meijer.duality.pdf) provides the iterator/observer duality background. André Staltz's [JavaScript Getter-Setter Pyramid](https://staltz.com/javascript-getter-setter-pyramid.html) contains the compact getter/getter and setter/setter formulas discussed in the session. The notation developed here is our working framework, not an official RxJS specification.
