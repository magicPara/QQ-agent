// 群聊定时合并回复。
//
// 4.0 的插件 API 没有直接暴露 Sender，因此由 src/tools.js 提供一个
// message.send-policy 能力接入点。插件只决定是否缓存以及何时调用当前会话
// 绑定的 send 回调，实际 OneBot 发送仍然经过原有 SendQueue、安全和限频逻辑。

const queues = new Map(); // chatKey -> { items, timer, send }
let apiRef = null;

function config() {
  return apiRef?.config?.() || {};
}

function clampInterval(value, fallback = 10000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(100, Math.min(3600000, Math.round(n)));
}

function parseRules(raw) {
  const rules = new Map();
  const text = String(raw ?? '').replace(/[，；]/g, (value) => value === '，' ? ',' : ';');
  for (const token of text.split(/[,;\n]+/)) {
    const item = token.trim();
    if (!item) continue;
    const match = item.match(/^(\d+)\s*[=:]\s*(off|关闭|\d+)$/i);
    if (!match) continue;
    rules.set(match[1], /^off|关闭$/i.test(match[2]) ? null : clampInterval(match[2]));
  }
  return rules;
}

function intervalFor(chatId) {
  const cfg = config();
  const rules = parseRules(cfg.groupRules);
  const listed = rules.has(String(chatId));
  if (String(cfg.scopeMode || 'all') === 'listed' && !listed) return null;
  if (listed) return rules.get(String(chatId));
  return clampInterval(cfg.defaultIntervalMs, 10000);
}

function triggerAtIds(triggerEntries, selfId) {
  const ids = [];
  const seen = new Set();
  for (const entry of Array.isArray(triggerEntries) ? triggerEntries : []) {
    if (entry?.self || entry?.isPoke) continue;
    const id = String(entry?.senderId ?? entry?.userId ?? '').trim();
    if (!/^\d+$/.test(id) || id === String(selfId || '') || seen.has(id)) continue;
    seen.add(id);
    ids.push(id);
  }
  return ids;
}

function queuedItems(messages, options, triggerEntries, selfId, replyEntry) {
  const explicit = Array.isArray(options?.atUserIds) ? [...options.atUserIds] : [];
  if (options?.atUserId !== undefined && options?.atUserId !== null) explicit.unshift(options.atUserId);
  if (explicit.some((id) => !/^[1-9]\d*$/.test(String(id).trim()) || String(id).trim() === String(selfId))) {
    throw new Error('收件人 QQ 号无效，尚未发送或入队；请填写实际收件人的数字 QQ 号。');
  }
  const explicitIds = [...new Set([
    ...explicit.map((id) => String(id).trim()).filter((id) => /^[1-9]\d*$/.test(id) && id !== String(selfId)),
  ])];
  const triggerIds = triggerAtIds(triggerEntries, selfId);
  const replySender = String(replyEntry?.senderId ?? replyEntry?.userId ?? '');
  const replyIds = /^[1-9]\d*$/.test(replySender) && replySender !== String(selfId) && !replyEntry?.self
    ? [replySender] : [];
  // 数组项序号与发言人顺序没有可靠对应关系。只接受明确目标、引用原作者，
  // 或唯一触发者；多人的目标不明确时拒绝入队，让模型补齐 QQ 号。
  const atUserIds = explicitIds.length ? explicitIds : replyIds.length ? replyIds
    : options?.replyToMessageId != null ? [] : triggerIds.length === 1 ? triggerIds : [];
  if (!atUserIds.length && (triggerIds.length > 1 || options?.replyToMessageId != null)) {
    throw new Error('回复对象不明确，尚未发送或入队。请每位收件人分别调用 send_message，并填写对应 atUserId，或引用该人的有效 replyToMessageId；不要按发言顺序猜人。');
  }
  const blocks = messages.map((text) => ({ text: String(text), atUserIds: [...atUserIds] }));
  return [{
    blocks,
    options: {
      replyToMessageId: options?.replyToMessageId ?? null
    }
  }];
}

async function flush(chatKey, state) {
  if (queues.get(chatKey) !== state) return;
  queues.delete(chatKey);
  if (state.timer) clearTimeout(state.timer);
  const blocks = state.items.flatMap((item) => item.blocks)
    .map((block) => ({ text: String(block?.text ?? '').trim(), atUserIds: block?.atUserIds || [] }))
    .filter((block) => block.text);
  if (!blocks.length) return;
  const merged = blocks.map((block) => block.text).join('\n\n');
  const firstOptions = state.items[0]?.options || {};
  const options = { ...firstOptions, atUserIdsByText: blocks };
  try {
    await state.send([merged], options);
    apiRef?.log?.('群 %s 已发送合并回复（%d 段，每段单独 @ 提问者）', chatKey, blocks.length);
  } catch (error) {
    // 工具调用已经向模型确认“已入队”，此处不能自动重发，避免 OneBot
    // 超时但实际已发送时产生重复消息。保留错误日志供控制台排查。
    apiRef?.error?.('群 %s 合并回复发送失败：%s', chatKey, error?.message ?? error);
  }
}

export function setup(api) {
  apiRef = api;
  api.log('群聊定时合并回复已注册');
}

export function promptSections({ kind, chatId } = {}) {
  if (kind !== 'group' || intervalFor(chatId) === null) return [];
  return [{ id: 'reply-target', priority: 80, content: '【合并回复对象】回复多人时，每位收件人分别调用 send_message，使用 atUserId 指定该人的 QQ 号，或用 replyToMessageId 引用该人的消息。每次调用的文本/数组都只属于同一位收件人。不要用字符串数组的顺序配对不同的人，不要在文本里手写 @ 昵称。插件会将回复合并，每段前加真实 @，段落间空一行。' }];
}

export const providers = {
  'message.send-policy': async function ({ chatKey, kind, chatId, messages, options, triggerEntries, selfId, replyEntry, send }) {
    // 需求限定为群聊；私聊永远沿用原始发送路径。
    if (kind !== 'group') return { handled: false };
    const interval = intervalFor(chatId);
    // listed 模式未列出的群，以及显式 off，均回退原始发送。
    if (interval === null) return { handled: false };

    let items;
    try {
      items = queuedItems(messages, options, triggerEntries, selfId, replyEntry);
    } catch (error) {
      return { handled: true, error: error.message };
    }

    let state = queues.get(chatKey);
    if (!state) {
      state = { items: [], timer: null, send };
      queues.set(chatKey, state);
      state.timer = setTimeout(() => { void flush(chatKey, state); }, interval);
      state.timer.unref?.();
    }
    state.items.push(...items);
    return {
      handled: true,
      sent: messages.map((text) => ({ text: String(text), messageId: null, queued: true })),
      note: `已加入 ${interval} 毫秒后的合并回复队列；每段只 @ 已确认的收件人。同批其他人的回复请分别调用 send_message 并填写各自 atUserId。`
    };
  }
};

async function flushAll() {
  const pending = [...queues.entries()];
  await Promise.allSettled(pending.map(([chatKey, state]) => flush(chatKey, state)));
}

export function deactivate() {
  // 停用或热重载时先尽力发送已缓存内容，再撤销旧定时器，避免静默丢回复。
  void flushAll();
}

export function dispose() {
  for (const state of queues.values()) if (state.timer) clearTimeout(state.timer);
  queues.clear();
  apiRef = null;
}
