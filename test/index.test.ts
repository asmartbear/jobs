import 'jest';      // Ref: https://jestjs.io/docs/en/expect#reference
import { ExecutionManager } from '../src/index'


test("construction", () => {
  const m = new ExecutionManager(3, false);
});

