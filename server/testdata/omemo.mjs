// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// The other side of TestOmemoInterop: an OMEMO device of libomemo.js, which
// Converse.js uses, in both versions, driven by the Go test one JSON line at
// a time on stdin, each answered on stdout. The test passes the library's
// module as MOCHI_LIBOMEMO.

import { createInterface } from 'node:readline'

// The library's logging goes to stderr; stdout carries the answers.
console.log = console.info = console.debug = console.warn = (...parts) => console.error(...parts)

const lib = await import(process.env.MOCHI_LIBOMEMO)
const versions = ['eu.siacs.conversations.axolotl', 'urn:xmpp:omemo:2']

const bytes = (b64) => {
  const raw = Buffer.from(b64, 'base64')
  return raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength)
}
const base64 = (value) =>
  typeof value === 'string'
    ? Buffer.from(value, 'binary').toString('base64')
    : Buffer.from(new Uint8Array(value)).toString('base64')
const raw = (key) => new Uint8Array(key).slice(1).buffer

// One store per version, sharing the identity, each with its own signed
// pre-key and one-time pre-keys.
const identity = await lib.KeyHelper.generateIdentityKeyPair()
const registration = lib.KeyHelper.generateRegistrationId()
const stores = {}
const bundles = {}
for (const [index, version] of versions.entries()) {
  const store = new lib.InMemoryStore()
  store.put('identityKey', identity)
  store.put('registrationId', registration)
  const signed = await lib.KeyHelper.generateSignedPreKey(identity, index + 1, version)
  store.storeSignedPreKey(signed.keyId, signed.keyPair)
  const prekey = await lib.KeyHelper.generatePreKey(100 + index)
  store.storePreKey(prekey.keyId, prekey.keyPair)
  stores[version] = store
  const current = version === 'urn:xmpp:omemo:2'
  bundles[version] = {
    identity: base64(current ? await lib.curvePubKeyToEd25519PubKey(identity.pubKey) : identity.pubKey),
    signed: { id: signed.keyId, key: base64(current ? raw(signed.keyPair.pubKey) : signed.keyPair.pubKey), signature: base64(signed.signature) },
    prekey: { id: prekey.keyId, key: base64(current ? raw(prekey.keyPair.pubKey) : prekey.keyPair.pubKey) },
    registration,
  }
}

const handle = async (request) => {
  const store = stores[request.version]
  const address = new lib.OMEMOAddress(request.name ?? 'go', request.device ?? 1)
  switch (request.op) {
    case 'bundle':
      return bundles[request.version]
    case 'begin': {
      const b = request.bundle
      await new lib.SessionBuilder(store, address, request.version).processPreKey({
        registrationId: b.registration,
        identityKey: bytes(b.identity),
        signedPreKey: { keyId: b.signed.id, publicKey: bytes(b.signed.key), signature: bytes(b.signed.signature) },
        preKey: { keyId: b.prekey.id, publicKey: bytes(b.prekey.key) },
      })
      return {}
    }
    case 'encrypt': {
      const result = await new lib.SessionCipher(store, address, request.version).encrypt(bytes(request.plain))
      return { body: base64(result.body), kex: result.type === 3 }
    }
    case 'decrypt': {
      const cipher = new lib.SessionCipher(store, address, request.version)
      const result = request.kex
        ? await cipher.decryptPreKeyWhisperMessage(bytes(request.body), 'binary')
        : await cipher.decryptWhisperMessage(bytes(request.body), 'binary')
      return { plain: base64(result.plaintext) }
    }
  }
  throw new Error('unknown op ' + request.op)
}

const lines = createInterface({ input: process.stdin })
for await (const line of lines) {
  let answer
  try {
    answer = await handle(JSON.parse(line))
  } catch (e) {
    answer = { error: String(e && e.message ? e.message : e) }
  }
  process.stdout.write(JSON.stringify(answer) + '\n')
}
