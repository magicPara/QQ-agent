// 回归：同批先收到 A 的招呼、再收到 B 的问题时，不能把 B 的答案 @ 给 A。
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'qq-batcher-target-'));
process.env.QQ_AGENT_DATA_DIR = dataDir;
const batcher = await import('../plugins/reply-batcher/index.js');
const { skillManager } = await import('../src/skills/manager.js');
const { buildToolDefs } = await import('../src/tools.js');
const { SendQueue } = await import('../src/sender.js');
const { OneBotClient } = await import('../src/onebot.js');

let cfg = { scopeMode: 'all', defaultIntervalMs: 100 };
const logs = [];
batcher.setup({ config: () => cfg, log() {}, error: (...args) => logs.push(args) });
skillManager.register({
  manifest: { id: 'reply-batcher', enabledByDefault: true, settings: {}, capabilities: ['message.send-policy'], requires: [] },
  providers: batcher.providers
});
const sendMessage = buildToolDefs().find((tool) => tool.id === 'send_message');
const calls = [];
const client = new OneBotClient({ wsUrl: 'ws://127.0.0.1:1', httpUrl: 'http://127.0.0.1:1' });
client.sendSegments = async (kind, id, segments) => {
  calls.push({ kind, id, segments });
  return { message_id: calls.length };
};
const sender = new SendQueue({ onebot: client, store: { appendSelf() {} } });
const trigger = [
  { mid: 101, senderId: '10001', text: '老吴' },
  { mid: 202, senderId: '10002', text: 'DAC 的外围电路设计很重要' },
  { senderId: '10001', isPoke: true, text: '拍了拍其他人' }
];
function ctx(chatId, entries = trigger) {
  return {
    chatKey: `group:${chatId}`, chatId, kind: 'group', selfId: '90000',
    triggerEntries: entries, sender, session: { sent: [] }, emit() {},
    store: { findByMid: (_key, mid) => trigger.find((entry) => String(entry.mid) === String(mid)) }
  };
}
const wait = () => new Promise((resolve) => setTimeout(resolve, 170));
try {
  // 与截图同样的无目标调用，以及等长数组，都必须报错，不能发送或记作入队。
  for (const messages of ['DAC 的回复', ['招呼', 'DAC 的回复']]) {
    const c = ctx('1');
    const r = await sendMessage.execute(c, { messages });
    assert.equal(r.isError, true);
    assert.match(r.content, /回复对象不明确/);
    assert.equal(c.session.sent.length, 0);
  }
  await wait();
  assert.equal(calls.length, 0);

  // 刻意颠倒发送顺序：以 QQ 号绑定，而非触发者序号。
  const c = ctx('2');
  await sendMessage.execute(c, { messages: 'DAC 的回复', atUserId: '10002' });
  await sendMessage.execute(c, { messages: '在呢，怎么说？', atUserId: '10001' });
  assert.equal(calls.length, 0);
  await wait();
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].segments, [
    { type: 'at', data: { qq: '10002' } },
    { type: 'text', data: { text: 'DAC 的回复' } },
    { type: 'text', data: { text: '\n\n' } },
    { type: 'at', data: { qq: '10001' } },
    { type: 'text', data: { text: '在呢，怎么说？' } }
  ]);

  // 引用消息确定原作者，不能用被引用文字里提到的人代替作者。
  await sendMessage.execute(ctx('3'), { messages: '引用答复', replyToMessageId: 202 });
  await wait();
  assert.deepEqual(calls[1].segments.filter((s) => s.type === 'at'), [{ type: 'at', data: { qq: '10002' } }]);
  assert.equal((await sendMessage.execute(ctx('4'), { messages: '无效引用', replyToMessageId: -999 })).isError, true);
  assert.equal((await sendMessage.execute(ctx('4'), { messages: '无效目标', atUserId: 'all' })).isError, true);

  // 多段内容仍属于显式收件人；唯一普通触发者可以自动补齐，拍他人的事件不算。
  await sendMessage.execute(ctx('5'), { messages: ['甲段', '乙段'], atUserId: '10002' });
  await wait();
  assert.deepEqual(calls[2].segments.filter((s) => s.type === 'at').map((s) => s.data.qq), ['10002', '10002']);
  await sendMessage.execute(ctx('6', [trigger[1], trigger[2]]), { messages: '单人答复' });
  await wait();
  assert.equal(calls[3].segments[0].data.qq, '10002');
  assert.equal(calls.length, 4);

  assert.equal((await batcher.providers['message.send-policy']({ kind: 'private' })).handled, false);
  cfg = { scopeMode: 'listed', groupRules: '7=off' };
  assert.equal((await batcher.providers['message.send-policy']({ kind: 'group', chatId: '7' })).handled, false);
  assert.equal((await batcher.providers['message.send-policy']({ kind: 'group', chatId: '8' })).handled, false);
  assert.equal(logs.length, 0);
  console.log('reply-batcher：多人歧义拒绝入队、显式 QQ/引用绑定、倒序回复、空行及旁路全部通过');
} finally {
  batcher.dispose();
  skillManager.unregister('reply-batcher');
  fs.rmSync(dataDir, { recursive: true, force: true });
}
