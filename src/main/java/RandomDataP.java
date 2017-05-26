/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.jet.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.Util.entry;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RandomDataP extends AbstractProcessor {

    private final int itemsPerSecond;
    private final boolean cooperative;
    private final String lagTrackerPrefix;

    private Random random = new Random();
    private long startNanoTime;
    private long startRealTime;
    private long numEmitted;
    private long interval;
    private final int minSleepTime;
    private IAtomicLong lagTracker;
    private long lagTrackerNextUpdate = Long.MIN_VALUE;

    public RandomDataP(int itemsPerSecond, boolean cooperative, String lagTrackerPrefix,
                       int minSleepTime) {
        this.itemsPerSecond = itemsPerSecond;
        this.cooperative = cooperative;
        this.lagTrackerPrefix = lagTrackerPrefix;
        this.interval = SECONDS.toNanos(1) / itemsPerSecond;
        this.minSleepTime = minSleepTime;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        startNanoTime = System.nanoTime();
        startRealTime = System.currentTimeMillis();
        lagTracker = context.jetInstance().getHazelcastInstance().getAtomicLong(
                lagTrackerPrefix + context.globalProcessorIndex());
    }

    @Override
    public boolean complete() {
        long nowNs = System.nanoTime();
        long shouldHaveEmitted = NANOSECONDS.toSeconds((nowNs - startNanoTime) * itemsPerSecond);
        if (nowNs >= lagTrackerNextUpdate) {
            lagTracker.set(shouldHaveEmitted - numEmitted);
            lagTrackerNextUpdate = nowNs + SECONDS.toNanos(1);
        }
        if (numEmitted == shouldHaveEmitted) {
            // this is what kafka connector after PR#442 does: if it has nothing to emit, it just returns
            if (cooperative) {
                return false;
            }
            // we've emitted enough, let's sleep for a while
            LockSupport.parkNanos(Math.min(interval, minSleepTime));
        }
        for (; numEmitted < shouldHaveEmitted; numEmitted++) {
            if (!tryEmit(entry(startRealTime + numEmitted * 1000 / itemsPerSecond, random.nextInt()))) {
                break;
            }
        }
        return false;
    }

    @Override
    public boolean isCooperative() {
        return cooperative;
    }
}
