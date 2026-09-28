import assert from 'node:assert/strict';
import test from 'node:test';
import { combineLatest, defer, EMPTY, NEVER, of } from 'rxjs';
import { delay, filter, map, scan, share, switchMap } from 'rxjs/operators';
import { TestScheduler } from 'rxjs/testing';

// These records implement the document's descriptive trace notation.
// The operators still receive their ordinary values, not these records.
const next = (t, value) => ({ t, kind: 'N', value });
const complete = t => ({ t, kind: 'C' });
const error = (t, reason = 'failure') => ({ t, kind: 'E', reason });

const double = value => value * 2;
const isEven = value => value % 2 === 0;
const add = (sum, value) => sum + value;

function failOnTwo(value) {
  // An RxJS error payload can be any thrown value.
  if (value === 2) throw 'failure';
  return value;
}

function allowUnlessTwo(value) {
  failOnTwo(value);
  return true;
}

function addUnlessTwo(sum, value) {
  return sum + failOnTwo(value);
}

function collect(observable, scheduler, events) {
  return observable.subscribe({
    next: value => events.push(next(scheduler.frame, value)),
    error: reason => events.push(error(scheduler.frame, String(reason))),
    complete: () => events.push(complete(scheduler.frame)),
  });
}

function subscriptionIntervals(source) {
  return source.subscriptions.map(({ subscribedFrame, unsubscribedFrame }) => ({
    subscribedFrame,
    unsubscribedFrame,
  }));
}

function verifyTrace(name, arrange, expected) {
  test(name, () => {
    const scheduler = new TestScheduler(() => {});
    const observed = [];
    let verifySubscriptions;
    scheduler.run(helpers => {
      verifySubscriptions = arrange(helpers, scheduler, observed);
    });
    assert.deepEqual(observed, expected);
    verifySubscriptions?.();
  });
}

verifyTrace('map preserves logical delivery time', ({ cold }, scheduler, events) => {
  const source = cold('10ms a 9ms b 9ms |', { a: 1, b: 3 });
  collect(source.pipe(map(double)), scheduler, events);
}, [next(10, 2), next(20, 6), complete(30)]);

verifyTrace('filter drops a value and forwards completion', ({ cold }, scheduler, events) => {
  const source = cold('10ms a 9ms b 9ms |', { a: 1, b: 2 });
  collect(source.pipe(filter(isEven)), scheduler, events);
}, [next(20, 2), complete(30)]);

verifyTrace('seeded scan emits updates but no initial seed', ({ cold }, scheduler, events) => {
  const source = cold('10ms a 9ms b 9ms |', { a: 1, b: 3 });
  collect(source.pipe(scan(add, 10)), scheduler, events);
}, [next(10, 11), next(20, 14), complete(30)]);

for (const [name, operator] of [
  ['map', map(failOnTwo)],
  ['filter', filter(allowUnlessTwo)],
  ['scan', scan(addUnlessTwo, 0)],
]) {
  verifyTrace(`${name} turns a user-function exception into error`, ({ cold }, scheduler, events) => {
    const source = cold('10ms a 9ms b 9ms c 9ms |', { a: 1, b: 2, c: 3 });
    collect(source.pipe(operator), scheduler, events);
    return () => assert.equal(source.subscriptions[0].unsubscribedFrame, 20);
  }, [next(10, 1), error(20)]);
}

verifyTrace('scan state is fresh per downstream subscription', (_helpers, scheduler, events) => {
  const source = of(1, 2).pipe(scan(add, 0));
  collect(source, scheduler, events);
  collect(source, scheduler, events);
}, [next(0, 1), next(0, 3), complete(0), next(0, 1), next(0, 3), complete(0)]);

verifyTrace('delay waits for pending values before completion', ({ cold }, scheduler, events) => {
  const source = cold('100ms a 199ms b 49ms |');
  collect(source.pipe(delay(500, scheduler)), scheduler, events);
}, [next(600, 'a'), next(800, 'b'), complete(800)]);

verifyTrace('delay of EMPTY completes immediately', (_helpers, scheduler, events) => {
  collect(EMPTY.pipe(delay(500, scheduler)), scheduler, events);
}, [complete(0)]);

verifyTrace('delay propagates errors immediately and cancels pending values', ({ cold }, scheduler, events) => {
  const source = cold('10ms a 9ms #', undefined, 'failure');
  collect(source.pipe(delay(100, scheduler)), scheduler, events);
}, [error(20)]);

verifyTrace('delay cancellation removes pending delivery and upstream subscription', ({ cold }, scheduler, events) => {
  const source = cold('10ms a 189ms |');
  const subscription = collect(source.pipe(delay(100, scheduler)), scheduler, events);
  scheduler.schedule(() => subscription.unsubscribe(), 50);
  return () => assert.equal(source.subscriptions[0].unsubscribedFrame, 50);
}, []);

verifyTrace('switchMap replaces the inner subscription and waits after outer completion', ({ cold }, scheduler, events) => {
  const innerA = cold('10ms a 29ms b 9ms |', { a: 'a1', b: 'a2' });
  const innerB = cold('10ms a 29ms b 9ms |', { a: 'b1', b: 'b2' });
  const outer = cold('a 24ms b 4ms |');
  const chooseInner = value => value === 'a' ? innerA : innerB;
  collect(outer.pipe(switchMap(chooseInner)), scheduler, events);
  return () => {
    assert.deepEqual(subscriptionIntervals(innerA), [{ subscribedFrame: 0, unsubscribedFrame: 25 }]);
    assert.deepEqual(subscriptionIntervals(innerB), [{ subscribedFrame: 25, unsubscribedFrame: 75 }]);
  };
}, [next(10, 'a1'), next(35, 'b1'), next(65, 'b2'), complete(75)]);

verifyTrace('switchMap cancellation closes outer and current inner subscriptions', ({ cold }, scheduler, events) => {
  const outer = cold('a 199ms |');
  const inner = cold('100ms a 9ms |');
  const chooseInner = () => inner;
  const subscription = collect(outer.pipe(switchMap(chooseInner)), scheduler, events);
  scheduler.schedule(() => subscription.unsubscribe(), 50);
  return () => {
    assert.equal(outer.subscriptions[0].unsubscribedFrame, 50);
    assert.equal(inner.subscriptions[0].unsubscribedFrame, 50);
  };
}, []);

test('share uses one overlapping connection, does not replay, and resets at zero subscribers', () => {
  const scheduler = new TestScheduler(() => {});
  const observed = { a: [], b: [], c: [] };
  let source;
  scheduler.run(({ cold }) => {
    source = cold('10ms a 9ms b 9ms c 9ms d 9ms e 9ms f 9ms |');
    const shared = source.pipe(share());
    const first = collect(shared, scheduler, observed.a);
    let second;
    let third;
    scheduler.schedule(() => { second = collect(shared, scheduler, observed.b); }, 15);
    scheduler.schedule(() => first.unsubscribe(), 25);
    scheduler.schedule(() => second.unsubscribe(), 35);
    scheduler.schedule(() => { third = collect(shared, scheduler, observed.c); }, 50);
    scheduler.schedule(() => third.unsubscribe(), 75);
  });
  assert.deepEqual(observed, {
    a: [next(10, 'a'), next(20, 'b')],
    b: [next(20, 'b'), next(30, 'c')],
    c: [next(60, 'a'), next(70, 'b')],
  });
  assert.deepEqual(subscriptionIntervals(source), [
    { subscribedFrame: 0, unsubscribedFrame: 35 },
    { subscribedFrame: 50, unsubscribedFrame: 75 },
  ]);
});

test('default share resets after synchronous completion', () => {
  let executions = 0;
  function createSource() {
    executions++;
    return of(1, 2);
  }
  const shared = defer(createSource).pipe(share());
  const observed = [];
  const observer = {
    next: value => observed.push(value),
    complete: () => observed.push('C'),
  };
  shared.subscribe(observer);
  shared.subscribe(observer);
  assert.equal(executions, 2);
  assert.deepEqual(observed, [1, 2, 'C', 1, 2, 'C']);
});

verifyTrace('combineLatest readiness, replacement, retained completed value, and final completion', ({ cold }, scheduler, events) => {
  const a = cold('10ms a 9ms b 29ms |', { a: 1, b: 2 });
  const b = cold('30ms x 9ms y 19ms z 9ms |');
  collect(combineLatest([a, b]), scheduler, events);
}, [next(30, [2, 'x']), next(40, [2, 'y']), next(60, [2, 'z']), complete(70)]);

test('combineLatest EMPTY plus NEVER remains open and silent', () => {
  const observed = [];
  const subscription = combineLatest([EMPTY, NEVER]).subscribe({
    next: value => observed.push(value),
    complete: () => observed.push('C'),
  });
  try {
    assert.deepEqual(observed, []);
    assert.equal(subscription.closed, false);
  } finally {
    subscription.unsubscribe();
  }
});

verifyTrace('combineLatest with an empty input waits for the other input to complete', ({ cold }, scheduler, events) => {
  collect(combineLatest([EMPTY, cold('50ms |')]), scheduler, events);
}, [complete(50)]);

verifyTrace('combineLatest processes equal-time notifications in order', ({ cold }, scheduler, events) => {
  const a = cold('a 9ms b 9ms |', { a: 1, b: 2 });
  const b = cold('x 9ms y 9ms |');
  collect(combineLatest([a, b]), scheduler, events);
}, [next(0, [1, 'x']), next(10, [2, 'x']), next(10, [2, 'y']), complete(20)]);
