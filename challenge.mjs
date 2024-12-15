// File: challenge.mjs

const events = {}; // { key: [{ id, timestamp, data }] }
const consumed = {}; // { key: { groupId: [eventId1, eventId2] } }
const waiting = {}; // { key: { groupId: [resolve, ...] } }

const TWO_MINUTES = 120000;
const BLOCKING_TIMEOUT = 30000;

// Cleanup function to remove expired events
setInterval(() => {
    const now = Date.now();
    for (const key in events) {
        events[key] = events[key].filter(event => now - event.timestamp < TWO_MINUTES);
    }
}, 10000); // Run every 10 seconds

// Helper: Mark events as consumed
function markAsConsumed(key, groupId, eventIds) {
    if (!consumed[key]) consumed[key] = {};
    if (!consumed[key][groupId]) consumed[key][groupId] = new Set();

    for (const id of eventIds) {
        consumed[key][groupId].add(id);
    }
}

// Helper: Get unconsumed events
function getUnconsumedEvents(key, groupId) {
    if (!events[key]) return [];
    const consumedSet = consumed[key]?.[groupId] || new Set();

    return events[key].filter(event => !consumedSet.has(event.id));
}

// POST /push
export async function push(key, data) {
    if (!key) throw new Error("Missing key");

    const event = { id: Date.now() + Math.random(), timestamp: Date.now(), data };
    if (!events[key]) events[key] = [];
    events[key].push(event);

    // Notify waiting consumers
    if (waiting[key]) {
        for (const groupId in waiting[key]) {
            while (waiting[key][groupId]?.length) {
                const resolve = waiting[key][groupId].shift();
                resolve(getUnconsumedEvents(key, groupId));
            }
        }
    }

    return "Event pushed";
}

// GET /blocking-get
export async function blockingGet(key, groupId) {
    if (!key || !groupId) throw new Error("Missing key or groupId");

    // Check for unconsumed events
    const unconsumed = getUnconsumedEvents(key, groupId);
    if (unconsumed.length > 0) {
        markAsConsumed(key, groupId, unconsumed.map(e => e.id));
        return unconsumed;
    }

    // Otherwise, wait for up to 30 seconds
    if (!waiting[key]) waiting[key] = {};
    if (!waiting[key][groupId]) waiting[key][groupId] = [];

    const promise = new Promise(resolve => {
        waiting[key][groupId].push(resolve);
        setTimeout(() => {
            const idx = waiting[key][groupId].indexOf(resolve);
            if (idx >= 0) waiting[key][groupId].splice(idx, 1);
            resolve([]);
        }, BLOCKING_TIMEOUT);
    });

    const result = await promise;
    markAsConsumed(key, groupId, result.map(e => e.id));
    return result;
}
