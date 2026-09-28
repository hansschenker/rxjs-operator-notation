# RxJS Operator Behavior Notation — Foundation v0.1

**A detailed session reference and a basis for further operator analysis**

| Field | Value |
| --- | --- |
| Established | 28 September 2026 |
| RxJS reference implementation | 7.8.2 |
| Main contributor | GPT-6 Astra |
| Concept development and review | Hans Schenker, collaboratively with GPT-6 Astra |
| Worked operators | `map`, `filter`, seeded `scan`, numeric `delay`, `switchMap`, default `share`, array-form `combineLatest` |
| Status | Working notation with explicit assumptions and executable example checks |
| Repository | [hansschenker/rxjs-operator-notation](https://github.com/hansschenker/rxjs-operator-notation) |

## Contents

1. [Purpose and central result](#1-purpose-and-central-result)
2. [How the idea developed](#2-how-the-idea-developed)
3. [Vocabulary and notation](#3-vocabulary-and-notation)
4. [Execution assumptions](#4-execution-assumptions)
5. [The general transition rule](#5-the-general-transition-rule)
6. [Seven worked operator descriptions](#6-seven-worked-operator-descriptions)
7. [Comparison of the seven operators](#7-comparison-of-the-seven-operators)
8. [A method for analyzing the next operator](#8-a-method-for-analyzing-the-next-operator)
9. [Validation and remaining boundaries](#9-validation-and-remaining-boundaries)
10. [Glossary](#10-glossary)
11. [Sources and attribution](#11-sources-and-attribution)

## 1. Purpose and central result

The goal is to explain RxJS operator behavior with a small, consistent notation. Begin with what flows over time, then add only the machinery needed to describe a particular operator accurately.

The original mental model was a sequence of time–value pairs:

$$
[\{t_1,v_1\},\{t_2,v_2\},\ldots]
$$

We refined this into timed notifications:

$$
\{t,N(v)\},\qquad \{t,E(e)\},\qquad \{t,C\}
$$

This immediately gives readable rules for value transformation, selection, accumulation, and delayed delivery. Higher-order mapping, sharing, and combination reveal the further information needed: remembered state, input identity, subscription lifetime, and event order.

The resulting general form is:

$$
\boxed{
(\text{previous state},\text{next event})
\xrightarrow{\delta}
(\text{new state},[\text{ordered actions}])
}
$$

The next event tells us what happened. The transition determines what to remember and what to do. The next state is the result of this transition; it is not its second input.

This document defines three levels of description:

| Level | What it describes | Typical example |
| --- | --- | --- |
| Delivery rule | How a notification becomes an output notification or no output | `map`, `filter` |
| Stateful reaction | How a notification updates memory and produces output | `scan`, `combineLatest` |
| Subscription and scheduling behavior | Which connections or scheduled tasks start, remain active, or stop | `delay`, `switchMap`, `share` |

An operator can need all three levels. A compact formula is a useful abbreviation when the omitted rules are understood and documented.

The intended outcome is a reusable behavior reference. The notation is not yet a complete executable specification of every RxJS implementation detail.

## 2. How the idea developed

### 2.1 From mathematical interfaces to delivered values

The session began with Ian Griffiths's video, *The Heart of Reactive Extensions for .NET*. Its central theme is that Rx's abstractions and compositional design grow out of mathematical relationships between collections and events. The supplied transcript moves from function composition at 01:13 to the function-accepting-a-function model at 02:46, the relationship with lists at 03:09, and the Observable/Observer abstractions at 04:52. [R1]

The initial interface sketch was:

$$
(T\rightarrow())\rightarrow()
$$

The inner function accepts a value. The outer function accepts that receiver. This describes the direction of value delivery and the higher-order shape of the interface; it does not yet encode notification order, completion, cancellation, or scheduling.

The getter/setter terminology made the direction clearer:

| Interface sketch | Reading |
| --- | --- |
| $()\rightarrow T$ | Getter: obtain a value |
| $T\rightarrow()$ | Setter or receiver: accept a value |
| $()\rightarrow(()\rightarrow T)$ | Getter of a getter: simplified iterable shape |
| $(T\rightarrow())\rightarrow()$ | Setter accepting a setter: simplified observable shape |

“Setter” is used here in a broad interface sense. Receiving a value does not necessarily mean assigning it to a property. Likewise, a function described as a callback is not thereby a getter: its role depends on its input and output. [R2, R3]

For accuracy, the getter/getter sketch describes an iterable producing an iterator-like getter. An iterator's simplified value-producing operation is the inner getter. Actual iterator protocols also encode exhaustion and errors.

The compact getter/getter and setter/setter formulas were located in André Staltz's article. Meijer's paper gives the fuller duality derivation using .NET interfaces and retains a disposable subscription handle. We do not attribute those exact compact formulas to an unverified quotation from Meijer. [R2, R3]

### 2.2 Add the notification protocol and a subscription handle

Define a tagged notification type conceptually as:

$$
\operatorname{Notification}\langle T\rangle
=N(T)+E(\mathcal{Error})+C
$$

Here, $+$ denotes a disjoint choice of cases. An error payload may be any thrown value; it need not be an instance of JavaScript's `Error` class.

A unified receiver has the conceptual shape:

$$
\operatorname{Notification}\langle T\rangle\rightarrow()
$$

This is the precise reading of the session's shorthand `(N | E | C) -> ()`. Ordinary RxJS observers expose separate `next`, `error`, and `complete` operations. We combine them into one tagged event vocabulary for analysis.

A fuller subscription sketch is:

$$
\operatorname{subscribe}:
\operatorname{Observer}\langle T\rangle\rightarrow\operatorname{Subscription}
$$

The returned handle makes cancellation part of the model. The `()` notation in the earlier sketches means no meaningful returned result. Those sketches describe an effectful delivery interface; they should not be read as a complete encoding of streams using only pure functions into a one-element type.

### 2.3 Connect the interface to a timeline

Subscribing establishes a relationship through which values can be observed. A timed notification trace records what happened along that relationship:

$$
\operatorname{trace}_s
=[\{t_1,N(v_1)\},\{t_2,N(v_2)\},\ldots]
$$

The interface explains how observation is established. The trace explains which notifications were delivered, in what order, and when. An Observable is the dataflow description; a trace describes a particular execution of that description.

This distinction is essential for cold sources, hot sources, switching, and sharing. One Observable description can participate in many subscriptions with different traces. [R4]

### 2.4 The progression through operators

The session developed the notation in this order:

1. `map`: apply a function to the value while retaining logical time.
2. `filter`: let a predicate choose between a notification and no output.
3. `scan`: carry accumulated state from one notification to the next.
4. `delay`: schedule delivery at a later logical time.
5. `switchMap`: identify and replace an active inner subscription.
6. `share`: identify multiple downstream subscriptions and one shared connection.
7. `combineLatest`: remember one latest value per input and require readiness.
8. Recognize the common structure as a state transition with actions.

The original question about changing $t$, $v$, or both remains useful. The larger model also describes cardinality, memory, connection structure, and lifetime.

## 3. Vocabulary and notation

### 3.1 Values, types, and notifications

| Symbol | Meaning |
| --- | --- |
| $T,U$ | Value types |
| $v:T$, $w:U$ | Actual values belonging to those types |
| $t$ | Logical delivery time on the chosen timeline |
| $N(v)$ | Next notification carrying $v$ |
| $E(e)$ | Error notification carrying $e$ |
| $C$ | Completion notification |
| $n$ | Any notification, whether next, error, or completion |
| $\varnothing$ | No output notification from this reaction |
| $[\ldots]$ | An ordered sequence of outputs or actions, according to context |
| $\bot$ | A distinguished “no value yet” marker |

Use $v$ for a value and $T$ for its type. This removes the ambiguity in the original `{t,T}` sketch.

`N`, `E`, and `C` are notation. Using these formulas does not require `materialize`, a `Notification` object, or an actual `{t, value}` wrapper in the RxJS pipeline.

$\varnothing$ is not an emitted `undefined`, a completion notification, or an `EMPTY` Observable. It means that this particular reaction produces no notification.

Likewise, $\bot$ is outside the value domain. An actual `undefined` emission still counts as a first value for `combineLatest`.

### 3.2 Input identity, subscription identity, and order

For several inputs or subscribers, attach a label:

$$
\{t,n\}_s
$$

The label must be defined by the example. It may name an input channel, such as $A$, or a particular subscription. When several subscriptions to the same source coexist or occur successively, use distinct identities such as `A#1` and `A#2`.

Do not confuse a source Observable with a subscription to that Observable. `switchMap` can subscribe again to the same Observable object and still create a new inner subscription.

When equal timestamps make the order significant, write:

$$
\{t,k,n\}_s
$$

$k$ records actual processing order among events that share a time. It is not an additional delay. A pair of notifications may have the same $t$ and different $k$ values.

### 3.3 Name user functions by their role

| Symbol | Type sketch | Role |
| --- | --- | --- |
| $f_v$ | $T\rightarrow U$ | Transform a value |
| $f_p$ | $T\rightarrow\operatorname{Boolean}$ | Decide whether a value passes |
| $f_r$ | $(S,T)\rightarrow S$ | Reduce a value into accumulated state |
| $f_o$ | $T\rightarrow\operatorname{Observable}\langle U\rangle$ | Project a value to an inner Observable |
| $f_t$ | $\operatorname{Time}\rightarrow\operatorname{Time}$ | Describe a time-coordinate transformation |

The role names are explanatory notation, not additional RxJS APIs. Our simple formulas omit index arguments. If a supplied function uses its index, include that index in the state and in the function application.

The inner projection is shown returning an Observable for clarity. RxJS 7 also accepts supported `ObservableInput` forms; an analysis using one must describe its subscription and timing behavior.

## 4. Execution assumptions

### 4.1 Fix the behavior being described

Pin the implementation version, operator form, arguments, and options. This reference uses RxJS 7.8.2 and the variants listed in the document metadata. Additional overloads require additional rules.

For example, a numeric duration and a `Date` argument to `delay` are different cases. Seeded and unseeded `scan` have different initialization behavior. Custom connectors and reset options change `share`.

### 4.2 A trace belongs to a subscription

For cold sources, subscribing creates the producer for that execution. For hot sources, the producer may already be active. Operators do not make every possible source cold simply because their result is lazy to subscribe.

For a cold inner trace with a relative emission at $\tau$, subscribing at $t_s$ places that emission at $t_s+\tau$, if the subscription is still active. A hot source keeps its existing timeline; subscribing later determines which subsequent events can be observed.

Subscriptions to synchronous sources can deliver values or complete before the subscribe operation returns. [R4, R5]

### 4.3 Define logical time and scheduling

Use one comparable time basis for events in a worked example. Time comes from sources and scheduler clocks; temporal operators use those facilities to schedule work.

When `map`, `filter`, or `scan` keeps $t$, the formula abstracts away ordinary processing cost and means that the operator introduces no scheduling boundary. It is not a physical zero-duration guarantee.

For a temporal operator, distinguish a task's due time from its actual physical execution time. A busy event loop can execute a task later. Virtual-time examples let us test the intended schedule without wall-clock variability. [R6]

### 4.4 Keep protocol, cancellation, and effects explicit

An open subscription may receive zero or more next notifications and at most one terminal error or completion. It may also remain open indefinitely. After termination or unsubscription it receives no further notifications through that subscription.

Unsubscription is a control action, not an emitted completion. Cancelling a subscription ends observation and runs its teardown. A shared producer may continue serving other subscriptions; cancellation does not imply that all underlying external work has stopped. [R4]

### 4.5 Declare state and its ownership

Every remembered fact that can change a later reaction belongs in the model: accumulators, latest-value slots, readiness flags, completion flags, input indices, pending tasks, active inner identities, subscriber membership, and reset state where relevant.

Describe who owns that state. `scan` normally creates accumulator state per downstream subscription. Default `share` coordinates state across subscriptions to one shared result Observable. Applying `share()` independently to the source twice creates two sharing groups.

An immutable reducer and stable seed make independent scan subscriptions easy to reason about. If a reducer mutates a seed object that is reused, shared object identity must also be represented.

### 4.6 Expose assumptions about supplied functions and values

The simple next-notification formulas assume successful, terminating, synchronous evaluation of the supplied function. We usually choose pure, deterministic functions for the examples.

When a function throws, reads external state, uses randomness, causes effects, or depends on its index, add that behavior to the analysis. RxJS does not require these functions to be pure.

Values are treated as stable for the worked examples. JavaScript object references are not automatically copied or frozen by emission. If later mutation matters, record value snapshots for observation or model references and mutation explicitly.

### 4.7 Preserve ordering inside reactions

A notification trace is ordered even when several events have the same timestamp. Synchronous propagation can nest, and subscription actions may immediately generate notifications.

Our compact operator rules describe ordinary cases without user-induced reentry into the same operator during an unfinished reaction. An exact model of reentrant cases must represent ordered substeps or an execution stack. It must preserve when state changes, when a subscriber is registered or closed, and when an effect, delivery, or teardown occurs.

A flat list of timestamps does not, by itself, specify those nested execution details. In particular, multicast delivery is ordered rather than a physically simultaneous broadcast.

### 4.8 Prediction is conditional on inputs

The model predicts reactions given the source events, their ordering, operator configuration, and supplied functions. It does not need every real-world source to be deterministic. Unpredictable external events simply enter as inputs to the model.

The model also does not predict CPU time, network success, or resource consumption unless those are explicitly introduced into the analysis.

## 5. The general transition rule

Let $\Sigma$ be the space of machine states, $\mathcal{E}$ the set of input events, and $\mathcal{A}$ the set of actions. Then:

$$
\delta:\Sigma\times\mathcal{E}\rightarrow\Sigma\times\mathcal{A}^{*}
$$

$\mathcal{A}^{*}$ means an ordered list of zero or more actions. It does not require a finite set of possible states; an operator may remember arbitrarily many values.

For a particular reaction:

$$
\delta(\sigma,e)=(\sigma',[a_1,\ldots,a_m])
$$

The transition is a stateful transducer description: consume an event, update memory, and describe actions. With all dependencies explicit, the transition calculation can be modeled as a pure function. Executing its actions is the responsibility of the surrounding execution model.

### 5.1 Events and actions

| Events the model may receive | Actions the model may produce |
| --- | --- |
| Input next, error, or completion | Emit next, error, or completion downstream |
| Timer or scheduled task firing | Schedule or cancel a task |
| A downstream subscriber joining | Register a subscriber or subscribe upstream |
| A downstream subscriber leaving | Remove a subscriber or unsubscribe upstream |
| An inner source notification | Forward it, update inner state, or change subscriptions |
| A modeled external result | Update state or describe a further effect |

For executable work, actions should carry sufficient identity: the target channel, subscription, or scheduled task. Cancelled tasks and closed subscriptions must not later deliver notifications through those cancelled relationships.

### 5.2 A reaction contract is not an arbitrary event queue

The list of actions is ordered. Interpreting `emit` or `subscribe` can synchronously cause downstream or inner reactions. Do not replace that behavior with a rule that always appends every reaction to a later queue.

If a state change must occur between two actions, split the reaction into phases with explicit intermediate state. This is how the simple notation can be refined for exact execution without pretending that every RxJS operation is atomic.

### 5.3 Relationship to scan

A reducer describes the state-update part:

$$
f_r(s,v)=s'
$$

Seeded `scan` adds the policy of emitting that accumulated state on each input next notification. The general transition rule also expresses scheduling, subscription changes, and effects. It does not imply that implementing an arbitrary operator as `scan` alone reproduces its full behavior.

### 5.4 Composition

At the delivery level, a next notification emitted by one stage becomes a next notification received by the next stage. Omitting a notification means that the next stage receives nothing from that reaction.

At the execution level, composition also connects cancellation, subscriptions, scheduled work, errors, and completion. Matching only the list of next values is therefore a weaker claim than matching the full operator behavior.

## 6. Seven worked operator descriptions

### 6.1 map: apply a value function

**Form:** `map(fv)`, with $f_v:T\rightarrow U$ and no index-dependent behavior in the example.

$$
\boxed{
\operatorname{map}(f_v):\quad
\{t,N(v)\}\longrightarrow\{t,N(f_v(v))\}
}
$$

The value may change type. Logical delivery time is retained. For ordinary successful evaluation, each input next notification produces one output next notification while the downstream subscription remains open.

| Input, with $f_v(v)=2v$ | Output |
| --- | --- |
| $\{10,N(1)\}$ | $\{10,N(2)\}$ |
| $\{20,N(3)\}$ | $\{20,N(6)\}$ |
| $\{30,C\}$ | $\{30,C\}$ |

No previous value is needed for this value-only projection. A function using the RxJS projection index requires a per-subscription index in the model.

Source errors and completion pass downstream. A projection exception produces an error notification and ends that subscription chain. Downstream cancellation unsubscribes upstream. `map` adds no sharing. [R7]

### 6.2 filter: use a predicate to select notifications

**Form:** `filter(fp)`, with $f_p:T\rightarrow\operatorname{Boolean}$.

$$
\boxed{
\operatorname{filter}(f_p):\quad
\{t,N(v)\}\longrightarrow
\begin{cases}
\{t,N(v)\} & f_p(v)=\operatorname{true}\\
\varnothing & f_p(v)=\operatorname{false}
\end{cases}
}
$$

Passing values retain their value and logical time. A rejected value generates no downstream notification and does not complete the stream.

| Input, with $f_p(v)$ meaning “is even” | Output |
| --- | --- |
| $\{10,N(1)\}$ | $\varnothing$ |
| $\{20,N(2)\}$ | $\{20,N(2)\}$ |
| $\{30,C\}$ | $\{30,C\}$ |

The model distinguishes a value function from a predicate even though both are supplied JavaScript functions. Source errors and completion pass through; a predicate exception errors the result. Cancellation propagates upstream, and `filter` adds no sharing. As with `map`, index-sensitive behavior needs an index in the state. [R8]

### 6.3 scan: remember and emit accumulated state

**Form:** `scan(fr, s0)`, with $f_r:(S,T)\rightarrow S$ and seed $s_0:S$.

$$
s_i=f_r(s_{i-1},v_i)
$$

$$
\boxed{
(s_{i-1},\{t_i,N(v_i)\})
\longrightarrow
(s_i,\{t_i,N(s_i)\})
}
$$

The reducer is the accumulator function. The seed and the reducer are separate arguments. The seed initializes memory; it is not automatically emitted.

| Input, with seed $10$ and addition reducer | Remembered state | Output |
| --- | --- | --- |
| Subscription starts | $10$ | $\varnothing$ |
| $\{10,N(1)\}$ | $11$ | $\{10,N(11)\}$ |
| $\{20,N(3)\}$ | $14$ | $\{20,N(14)\}$ |
| $\{30,C\}$ | — | $\{30,C\}$ |

Each ordinary downstream subscription has its own accumulation. Sharing after the scan can let downstream consumers observe one shared accumulation; that sharing boundary must be explicit.

Source completion does not add a final next notification. Source errors and completion propagate, and a reducer exception errors the result. Cancellation unsubscribes upstream. The unseeded overload has different first-value behavior and requires a separate initialization rule. [R9]

### 6.4 delay: schedule delivery later

**Form:** `delay(d, scheduler)`, for a fixed numeric duration $d>0$. The scheduler is stated for each example; RxJS uses `asyncScheduler` by default.

Define the descriptive time transformation:

$$
f_t(t)=t+d
$$

$$
\boxed{
\operatorname{delay}(d):\quad
\{t,N(v)\}\longrightarrow\{t+d,N(v)\}
}
$$

Read the arrow as a scheduled delivery, conditional on the subscription remaining open. The value is retained. This delivery rule describes next notifications, not all three notification kinds.

A fuller reaction model remembers pending deliveries $P$ and whether the source has completed:

| Trigger | Reaction |
| --- | --- |
| $\{t,N(v)\}$ | Register a pending delivery and schedule it for $t+d$ |
| Pending delivery becomes due | Deliver $N(v)$ if still valid; remove the pending item |
| Source $C$ | Mark the source complete; complete when no deliveries remain pending |
| Source $E(e)$ | Emit $E(e)$ immediately and cancel pending deliveries |
| Downstream cancellation | Unsubscribe upstream and cancel pending deliveries; emit no $C$ |

After a due next notification is delivered, completion is allowed only if the source is complete, no deliveries remain pending, and downstream is still open.

For `delay(500)`:

| Source notification | Output notification |
| --- | --- |
| $\{100,N(a)\}$ | $\{600,N(a)\}$ |
| $\{300,N(b)\}$ | $\{800,N(b)\}$ |
| $\{350,C\}$ | $\{800,C\}$, after the last value |

For a finite, normally completing trace under the ideal schedule, the completion time is the later of source completion and the last pending delivery time. An empty source completes immediately because there are no pending values.

The source subscription starts immediately. Delaying downstream delivery does not delay the start of upstream work. `delay` adds no sharing. A zero duration still uses scheduling with the default scheduler and should not be silently rewritten as synchronous identity. The `Date` overload is outside this card. [R10]

### 6.5 switchMap: keep the latest inner subscription

**Form:** `switchMap(fo)`, with $f_o:T\rightarrow\operatorname{Observable}\langle U\rangle$ in this analysis.

For each downstream subscription, remember the active inner subscription $a$ and whether the outer source has completed. An outer next notification causes an ordered change:

$$
\boxed{
\{t_i,N(v_i)\}_{outer}
\longrightarrow
[\operatorname{unsubscribe}(a),
\operatorname{subscribe}(r_i,f_o(v_i),t_i)]
}
$$

$r_i$ is a fresh inner subscription identity. The first action is omitted if no inner is active. In the actual order, the old inner is unsubscribed before the projection is evaluated. Register the new active inner before subscribing it, so immediate inner notifications can be handled correctly.

The delivery policy is:

$$
\{u,N(w)\}_{r}
\longrightarrow
\begin{cases}
\{u,N(w)\}_{out} & r\text{ is the active inner}\\
\varnothing & \text{otherwise}
\end{cases}
$$

The second branch describes which potential source values are excluded. A cancelled inner subscription is actually closed; the operator does not need to keep receiving and filtering its later notifications.

Consider cold inner sources that emit $x_1$ at relative time $10$, $x_2$ at $40$, and complete at $50$. The outer emits $a$ at $0$, $b$ at $25$, and completes at $30$:

| Time | Event | Output or subscription action |
| --- | --- | --- |
| 0 | Outer emits $a$ | Subscribe to inner $a$ |
| 10 | Inner $a$ emits $a_1$ | $\{10,N(a_1)\}$ |
| 25 | Outer emits $b$ | Cancel inner $a$; subscribe to inner $b$ |
| 30 | Outer completes | Wait for active inner $b$ |
| 35 | Inner $b$ emits $b_1$ | $\{35,N(b_1)\}$ |
| 65 | Inner $b$ emits $b_2$ | $\{65,N(b_2)\}$ |
| 75 | Inner $b$ completes | $\{75,C\}$ |

The potential $a_2$ delivery at time $40$ is cancelled. The already delivered $a_1$ remains part of the output history.

Completion requires both outer completion and no active inner. An outer error, active-inner error, or projection exception errors the result and closes remaining subscriptions. Downstream cancellation closes outer and inner subscriptions. Switching ends this inner subscription; an independently shared inner producer may keep running for other consumers. `switchMap` itself introduces no sharing. [R11]

### 6.6 share: coordinate consumers through a shared connection

**Form:** default `share()` in RxJS 7.8.2: a `Subject` connector, with reset on error, completion, and zero downstream subscribers enabled.

For one shared result Observable, remember the active downstream subscribers $A$ and its upstream connection. The delivery rule is:

$$
\boxed{
\{t,n\}_{upstream}
\longrightarrow
[\{t,n\}_s\mid s\in A]
}
$$

Each active recipient gets the same notification at the same logical time. This does not mean the value is cloned or that recipients execute simultaneously. When order matters, record the order of recipients and whether each is still open at its actual delivery step.

The connection rules are:

| Trigger | Reaction |
| --- | --- |
| First subscriber joins | Register it before starting the upstream subscription |
| Another subscriber joins | Register it and reuse the current connection |
| One subscriber leaves | Remove it; retain the connection while others remain |
| Last subscriber leaves | Reset and unsubscribe from the upstream connection |
| Source errors or completes | Notify the current group, end its subscriptions, and make a fresh connection possible for later subscribers |

For an active connection with $A=\{a,b\}$:

$$
\{20,N(x)\}_{upstream}
\longrightarrow
[\{20,N(x)\}_a,\{20,N(x)\}_b]
$$

A subscriber joining that connection later receives future notifications only. Default `share` does not replay previous values. After the last subscriber leaves, a later subscriber starts a new source subscription.

Two subscriptions made successively to a synchronously completing shared source can start two executions: the first execution has completed and reset before the second subscriber joins. Sharing depends on overlapping subscription lifetimes and the configured reset policy.

For reentrant terminal cases, refine the summary table: RxJS 7.8.2 resets its connection state before notifying the captured old subject of an error or completion. A subscription made during that notification can therefore start a fresh connection. Custom connectors and reset options require their own analysis. [R12]

### 6.7 combineLatest: remember each input and emit when ready

**Form:** `combineLatest([O1, ..., On])`, with at least one input and no result selector or explicit scheduling overload in this card.

The output value is a tuple in input order. Inputs can have different types. Each result subscription has its own input subscriptions and remembered values.

Let:

$$
L=[\ell_1,\ldots,\ell_n],\qquad
L_0=[\bot,\ldots,\bot]
$$

Also remember the set $D$ of completed inputs. Upon $\{t,N(v)\}_i$, update just the corresponding slot:

$$
L'=L[i\leftarrow v]
$$

The compact next rule is:

$$
\boxed{
(L,\{t,N(v)\}_i)
\longrightarrow
\begin{cases}
(L',\{t,N(L')\}_{out}) & \text{all slots of }L'\text{ are filled}\\
(L',\varnothing) & \text{otherwise}
\end{cases}
}
$$

The completion set $D$ is unchanged by this next rule. Before readiness, values are still remembered and can replace earlier values from the same input. After readiness, every next notification triggers a fresh tuple, including when its value equals the previously remembered value.

For two ready inputs:

$$
\begin{aligned}
\{t,N(a)\}_A&\longrightarrow\{t,N([a,b_{latest}])\}_{out}\\
\{t,N(b)\}_B&\longrightarrow\{t,N([a_{latest},b])\}_{out}
\end{aligned}
$$

| Input notification | Remembered latest values | Output |
| --- | --- | --- |
| $\{10,N(1)\}_A$ | $[1,\bot]$ | $\varnothing$ |
| $\{20,N(2)\}_A$ | $[2,\bot]$ | $\varnothing$ |
| $\{30,N(x)\}_B$ | $[2,x]$ | $\{30,N([2,x])\}$ |
| $\{40,N(y)\}_B$ | $[2,y]$ | $\{40,N([2,y])\}$ |
| $\{50,C\}_A$ | $[2,y]$ | $\varnothing$ |
| $\{60,N(z)\}_B$ | $[2,z]$ | $\{60,N([2,z])\}$ |
| $\{70,C\}_B$ | — | $\{70,C\}$ |

On input completion, set $D'=D\cup\{i\}$ and retain its latest value. Emit $C$ when all inputs are in $D'$. Completion does not itself generate another next tuple. Any input error errors the result and unsubscribes the other inputs. Downstream cancellation closes all input subscriptions. [R13]

**Pinned-version detail:** in RxJS 7.8.2, an input completing without a value makes tuple emission impossible, but does not by itself complete the result. Completion still waits for all inputs. `combineLatest([EMPTY, NEVER])` therefore stays open and silent until cancellation. Some prose documentation describes a different empty-input rule; this reference follows the inspected 7.8.2 implementation and executed checks.

The zero-input case, `combineLatest([])`, completes immediately upon subscription.

Input subscriptions are established in input order. Synchronous inputs can emit while those subscriptions are being established. Equal-time notifications are processed in delivery order; they are not automatically merged into one atomic update. `combineLatest` adds no sharing and uses no timer in the ordinary form covered here.

## 7. Comparison of the seven operators

The following table summarizes ordinary next behavior. It does not replace the lifecycle rules above.

| Operator | Remembered information | Next-notification policy | Timing and connections |
| --- | --- | --- | --- |
| `map` | No value history; index if used | Apply $f_v$ | Keep incoming logical time |
| `filter` | No value history; index if used | Forward if $f_p$ passes | Keep time of retained notifications |
| `scan` | Accumulator; index if used | Update state and emit it | One accumulation per result subscription unless explicitly shared |
| `delay` | Pending deliveries and source completion | Schedule the unchanged value | Due at $t+d$; errors and cancellation can remove pending output |
| `switchMap` | Active inner identity and outer completion | Forward active-inner values | Replace the inner connection on each outer next |
| `share` | Subscribers, connection, connector, reset state | Deliver to the active group | Coordinate an upstream connection across consumers |
| `combineLatest` | Latest slots, readiness, completed inputs | Update one slot and emit the tuple when ready | Any input can trigger output; subscribe to all inputs |

The initial “value, time, or both” classification remains a good entry point. State and subscription rules explain behavior that cannot be recovered from the shape of one output value alone.

## 8. A method for analyzing the next operator

Use the [operator analysis template](../templates/operator-analysis.md) as the working card. Keep its fields explicit even when a particular field has the answer “none” or “unchanged.”

### 8.1 Establish the contract

Record the version, operator form, parameters, supplied functions, source kinds, input identities, scheduler, and subscription start times. State whether any input or output is shared and which reset policy applies.

Give a short type sketch. Keep the user-defined function's role separate from the operator policy. For example, a projection chooses an inner Observable; the flattening operator determines how inner subscriptions coexist or replace one another.

### 8.2 List triggers and remembered state

List every trigger that can change behavior: source or inner notifications, notifier notifications, timer firings, subscriber joins, subscriber departures, or modeled external events.

Then list the state required to react: previous value, count, queue, current inner identity, active count, latest-value table, group registry, pending task identities, or terminal flags. Specify initialization, ownership, and reset.

### 8.3 Write the next rule and the other reactions

Start with the smallest useful formula. If it hides an essential dependency, add state or input identity. If a reaction starts later work or changes connections, make the action explicit.

Handle completion, error, exceptions from supplied functions, and downstream cancellation independently. If an operator consumes a notifier, specify separately what notifier next, error, and completion mean; those roles vary by operator.

### 8.4 Add one discriminating example

Choose a trace that distinguishes this policy from related policies. A useful example should reveal why the operator exists.

For flattening policies, use inners whose lifetimes overlap. For remembered-value policies, make an input emit twice before another input becomes ready. For temporal policies, include both a burst and a boundary. For sharing, include staggered joins and departures.

State the actual event order at equal times. Include subscription intervals when the operator changes or shares connections.

### 8.5 Validate the concrete uncertainty

Check the expected output against RxJS 7.8.2. Check subscription lifetimes when cancellation or sharing is part of the claim. Add boundary cases that resolve a specific ambiguity; do not substitute a large number of redundant next-value tests for a missing lifecycle rule.

Keep implementation evidence and interpretation separate. Record whether a statement is an assumption, a source-derived rule, or an executed observation.

### 8.6 Useful next analyses

| Candidate | Additional behavior to describe |
| --- | --- |
| `take` | Count-based early completion and upstream cancellation |
| `reduce` | Accumulation whose output is triggered by completion |
| `distinctUntilChanged` | Previous-value memory and comparison |
| `debounceTime` | Replaceable pending value and timer |
| `mergeMap`, `concatMap`, `exhaustMap` | Overlap, queueing, and ignore-while-busy policies |
| `withLatestFrom` | Several remembered inputs with one designated output trigger |
| `zip` | Per-input queues and matching |
| `catchError`, bounded `retry` | Recovery and new subscription attempts |
| `buffer`, `window`, `groupBy` | Collections or new output channels with their own lifetimes |
| `observeOn`, `subscribeOn` | Scheduled delivery versus scheduled subscription |
| `finalize` | Teardown effects across completion, error, and cancellation |

These are proposed next cards, not operators already specified or validated by this document.

## 9. Validation and remaining boundaries

### 9.1 Executed evidence

The repository pins `rxjs` to `7.8.2`. Its test suite contains these 19 targeted cases:

| Area | Executed checks |
| --- | --- |
| `map` | Time-preserving transformation; projection exception |
| `filter` | Rejection with normal completion; predicate exception |
| `scan` | Seeded updates without an initial seed emission; reducer exception; fresh state on another subscription |
| `delay` | Pending values before completion; immediate completion of empty input; immediate error with pending cancellation; explicit unsubscription |
| `switchMap` | Inner replacement and completion after the outer; cancellation of outer and active inner |
| `share` | Overlapping consumers, no replay, and reset after the last departure in one scenario; reset after synchronous completion |
| `combineLatest` | Readiness and retained completed-input value; `EMPTY` plus `NEVER`; empty input with delayed completion of another input; equal-time input order |

All 19 checks passed when this reference was prepared. Timed cases use virtual time; synchronous boundary checks use the same pinned package directly.

To reproduce, from the repository root:

```sh
npm ci
npm test
```

The checked examples establish concrete evidence for the documented cases. They are not an exhaustive conformance suite, and every prose rule has not been tested in every configuration.

### 9.2 Boundaries of version 0.1

The working model does not yet define a complete interpreter for nested execution, arbitrary effects, scheduler internals, or resource ownership. Those details must be added when an example depends on them.

The seven cards do not cover every overload. In particular, additional sharing policies, `delay` with a `Date`, unseeded `scan`, and deprecated projection overloads are separate analyses.

The state-transition structure is general enough to organize further investigations. Successfully describing these examples does not constitute a proof of equivalence for every RxJS operator or every JavaScript program that can be placed inside one.

## 10. Glossary

| Term | Meaning in this reference |
| --- | --- |
| Observable | A description of how observation connects to a producer |
| Trace | The ordered notifications recorded for a particular execution |
| Notification | A next, error, or completion event |
| Logical time | The timeline coordinate used by the analysis |
| Due time | When a scheduled action is intended to become eligible to execute |
| Subscription | A particular ongoing observation relationship |
| Teardown | Cleanup associated with ending a subscription |
| Cold source | A source creating a producer for each subscription |
| Hot source | A source whose producer exists independently of a particular subscription |
| Sharing | Coordination of consumers through a common upstream connection |
| State | Information remembered because it can affect a later reaction |
| Reducer | A function computing a new accumulated state from state and an input value |
| Trigger | An event that causes the operator to react |
| Action | A described delivery, scheduling operation, connection change, or effect |
| Transition function | A rule relating previous state and an event to new state and actions |
| Stateful transducer | A process that receives events, remembers information, and produces outputs or actions |
| Readiness | The condition required before an output can be produced |
| Reentrancy | A new invocation entering an operator while an earlier reaction is unfinished |
| Policy | The rule deciding selection, timing, overlap, cancellation, or distribution |

## 11. Sources and attribution

The notation, examples, comparison, and analysis workflow are the synthesis developed during this session. The references below ground the interface background and the RxJS behavior. Links to package source are pinned to 7.8.2; general documentation may contain historical text or change later.

- **[R1] Ian Griffiths:** [The Heart of Reactive Extensions for .NET](https://www.youtube.com/watch?v=9B4LRxUKpd4). Session discussion used the transcript supplied by Hans Schenker.
- **[R2] Erik Meijer:** [Subject/Observer is Dual to Iterator](https://csl.stanford.edu/~christos/pldi2010.fit/meijer.duality.pdf). The .NET interface derivation and retained disposal handle.
- **[R3] André Staltz:** [JavaScript Getter-Setter Pyramid](https://staltz.com/javascript-getter-setter-pyramid.html), 18 December 2018. Source of the compact getter/getter and setter/setter presentation discussed here.
- **[R4] RxJS:** [Glossary and semantics](https://rxjs.dev/guide/glossary-and-semantics).
- **[R5] RxJS:** [Observable guide](https://rxjs.dev/guide/observable).
- **[R6] RxJS:** [Scheduler guide](https://rxjs.dev/guide/scheduler).
- **[R7] RxJS 7.8.2:** [map.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/map.ts).
- **[R8] RxJS 7.8.2:** [filter.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/filter.ts).
- **[R9] RxJS 7.8.2:** [scan.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/scan.ts) and [scanInternals.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/scanInternals.ts).
- **[R10] RxJS 7.8.2:** [delay.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/delay.ts), [delayWhen.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/delayWhen.ts), and [mergeInternals.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/mergeInternals.ts).
- **[R11] RxJS 7.8.2:** [switchMap.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/switchMap.ts).
- **[R12] RxJS 7.8.2:** [share.ts](https://unpkg.com/rxjs@7.8.2/src/internal/operators/share.ts) and [Subject.ts](https://unpkg.com/rxjs@7.8.2/src/internal/Subject.ts).
- **[R13] RxJS 7.8.2:** [combineLatest.ts](https://unpkg.com/rxjs@7.8.2/src/internal/observable/combineLatest.ts).

Prepared as the foundation for continued operator analysis. Keep the implementation baseline, configuration assumptions, and validation evidence visible when extending this reference.
