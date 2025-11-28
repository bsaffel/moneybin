# Security Tradeoffs and Threat Model

## The Fundamental Tradeoff

MoneyBin Sync faces a fundamental security tradeoff that affects all similar services:

### The Problem

To provide automatic bank sync, we need to:

1. **Store Plaid access tokens** securely (can't ship to clients)
2. **Call Plaid API** with these tokens (must happen server-side)
3. **Receive plaintext data** from Plaid (API returns unencrypted JSON)
4. **Convert to user's format** (Parquet serialization)
5. **Encrypt for user** (only user can decrypt)

**The tradeoff**: The server **must** see plaintext data briefly during steps 3-5.

### The Question

**Can the server process Plaid data without ever seeing plaintext?**

**Answer: No** - not with current practical technology. Here's why:

## Alternative Approaches Considered

### Option 1: Client-Side Plaid Integration (Current Free Tier)

```
User → Plaid Link → Plaid API → Client (has token) → Local Processing
```

**Pros:**

- ✅ Server never sees anything
- ✅ Complete client control
- ✅ True zero-knowledge

**Cons:**

- ❌ User must manage Plaid credentials
- ❌ User must handle token refresh/expiration
- ❌ No automatic sync when client offline
- ❌ Each device needs separate token
- ❌ Token stored on user's device (compromise risk)

**Verdict:** Good for free tier, but not suitable for "automatic sync" paid feature.

### Option 2: Server-Side with Transient Plaintext (Chosen Approach)

```
User → MoneyBin Sync → Plaid API → Server (brief plaintext) → Encrypt → User
```

**Pros:**

- ✅ Automatic sync works when client offline
- ✅ Plaid tokens stored securely server-side
- ✅ Token refresh handled server-side
- ✅ Multi-device sync works seamlessly
- ✅ Encrypted data at rest
- ✅ User can decrypt anytime

**Cons:**

- ⚠️ Server sees plaintext briefly (during encryption)
- ⚠️ Requires trusting server operator during processing
- ⚠️ Vulnerable to active server compromise (not passive breach)

**Verdict:** Best practical compromise for automatic sync service.

### Option 3: Trusted Execution Environment (TEE)

```
User → MoneyBin Sync → Intel SGX/AWS Nitro Enclave → Encrypt → User
```

Run encryption in hardware-isolated secure enclave that can't be accessed even by server operators.

**Pros:**

- ✅ Even server operators can't see plaintext
- ✅ Cryptographically verified execution
- ✅ Best of both worlds (automatic + secure)

**Cons:**

- ❌ Extremely complex to implement
- ❌ Expensive (specialized hardware)
- ❌ Limited availability (not all clouds)
- ❌ Still requires some trust in hardware manufacturer
- ❌ Overkill for initial MVP

**Verdict:** Possible future enhancement, not practical for v1.

### Option 4: Homomorphic Encryption

Process encrypted data without decrypting it.

**Pros:**

- ✅ True zero-knowledge possible
- ✅ Mathematically secure

**Cons:**

- ❌ Extremely slow (orders of magnitude slower)
- ❌ Not practical for real-time applications
- ❌ Can't convert formats while encrypted
- ❌ Research technology, not production-ready

**Verdict:** Not practical for this use case.

### Option 5: Client-Side Only with Manual Export

User exports data from Plaid dashboard manually.

**Pros:**

- ✅ No server involvement
- ✅ Complete user control

**Cons:**

- ❌ Not automatic sync
- ❌ Poor user experience
- ❌ Defeats purpose of paid tier

**Verdict:** This is essentially the free tier approach.

## Chosen Security Model

### What We Implement: Server-Side Encryption with Clear Disclosure

**Honest security claims:**

1. ✅ **Server encrypts immediately** - no plaintext stored
2. ✅ **Server cannot decrypt your stored data** - only you have the key
3. ✅ **Database breach protection** - stored data is encrypted
4. ✅ **Network interception protection** - E2E encryption
5. ⚠️ **Server sees plaintext briefly** - during Plaid→Parquet→Encrypt process
6. ⚠️ **Requires server trust** - during active processing

### What We're Protected Against

| Threat | Protected? | How |
|--------|-----------|-----|
| Database breach | ✅ Yes | Only encrypted data stored |
| Disk compromise | ✅ Yes | No plaintext persisted |
| Network sniffing | ✅ Yes | TLS + E2E encryption |
| Subpoena for stored data | ✅ Yes | Server can't decrypt |
| Passive server compromise | ✅ Yes | Encrypted at rest |
| Active server compromise | ⚠️ Partial | Could intercept during processing |
| Malicious server operator | ⚠️ No | Has access during processing |
| Memory dump during processing | ⚠️ No | Plaintext briefly in RAM |

### What Requires Trust

**You must trust that the MoneyBin Sync server:**

1. ✅ **Encrypts immediately** (we can prove via open source code)
2. ✅ **Doesn't store plaintext** (we can prove via code + audits)
3. ⚠️ **Doesn't log plaintext** (auditable, but requires trust)
4. ⚠️ **Doesn't send plaintext elsewhere** (auditable, but requires trust)
5. ⚠️ **Runs the audited code** (can verify via reproducible builds)

**This is similar to:**

- Plaid itself (they see your banking credentials!)
- Email services (see plaintext emails)
- Cloud password managers that do server-side processing

**This is better than:**

- Most SaaS services (they store plaintext indefinitely)
- Traditional financial aggregators (often don't encrypt at rest)

**This is not as good as:**

- True zero-knowledge services (Signal, certain password managers)
- Client-side only processing (MoneyBin free tier)

## Mitigation Strategies

### What We Do to Minimize Risk

1. **Minimize plaintext window**

   ```python
   # Encrypt immediately after Parquet conversion
   data = fetch_from_plaid()  # plaintext
   parquet = to_parquet(data)  # still plaintext in memory
   encrypted = encrypt(parquet, user_public_key)  # NOW encrypted
   secure_zero_memory(data, parquet)  # Zero original memory
   return encrypted  # Only encrypted data leaves function
   ```

2. **No persistence of plaintext**
   - No database writes before encryption
   - No disk caching
   - No debug logs with plaintext
   - No error messages with plaintext

3. **Memory security**
   - Zero memory after encryption
   - No core dumps with sensitive data
   - Memory encryption (OS-level if available)

4. **Code transparency**
   - Open source encryption logic
   - Regular security audits
   - Reproducible builds
   - Third-party code review

5. **Access controls**
   - Minimal server operator access
   - Audit logging of all operations
   - Anomaly detection
   - Regular security monitoring

6. **Future: Secure enclaves**
   - Phase 2 could add Intel SGX support
   - Reduces trust requirements
   - Increases cost and complexity

## Communicating to Users

### Honest Marketing Language

**Good (honest):**

- ✅ "Your financial data is encrypted end-to-end"
- ✅ "We can't decrypt your stored data - only you can"
- ✅ "Your data is encrypted before storage and transmission"
- ✅ "We process data securely and encrypt immediately"

**Bad (misleading):**

- ❌ "We never see your financial data" (false - we see it briefly)
- ❌ "True zero-knowledge encryption" (not technically accurate)
- ❌ "We can't access your data" (we can, during processing)
- ❌ "Complete privacy guaranteed" (requires trust during processing)

### Recommended Disclosure

> **Security Model:**
>
> When you sync your bank accounts, MoneyBin Sync:
>
> 1. Fetches your transaction data from Plaid on your behalf
> 2. Processes and encrypts this data using your encryption key
> 3. Sends you the encrypted data (only you can decrypt it)
>
> **What this means:**
>
> - ✅ Your stored data is encrypted - we can't read it later
> - ✅ If our database is breached, your data is encrypted
> - ✅ Only you have the decryption key
> - ⚠️ We do see your data briefly while encrypting it for you
> - ⚠️ You trust us to encrypt immediately and not keep copies
>
> **Alternative (Free Tier):**
>
> - Use MoneyBin Client-Only mode
> - No data ever sent to our servers
> - Manual import from CSV/OFX files
> - Complete control, but less convenient

## Comparison to Similar Services

### Plaid (What We Build On)

**Trust model:**

- ❌ Sees all your banking credentials
- ❌ Sees all your financial data
- ❌ Stores data for their service
- ✅ Heavily regulated and audited
- ✅ Bank-grade security

**Our improvement:** We only see processed data, never banking credentials.

### Traditional Financial Aggregators (Mint, YNAB, etc.)

**Trust model:**

- ❌ Store plaintext data indefinitely
- ❌ Can query your data anytime
- ❌ May share data with partners
- ⚠️ Encryption at rest varies

**Our improvement:** We encrypt immediately and can't decrypt later.

### Password Managers (1Password, Bitwarden)

**Trust model:**

- ✅ True zero-knowledge (for SRP models)
- ✅ Never see master password
- ✅ Never see encryption keys
- ✅ Never see plaintext data
- ✅ Can't decrypt stored data

**Why we can't match this:** Password managers don't need to process external API data.

### Signal, ProtonMail (True Zero-Knowledge)

**Trust model:**

- ✅ E2E encryption
- ✅ Server only relays encrypted messages
- ✅ Server never decrypts

**Why we can't match this:** They don't integrate with third-party APIs that return plaintext.

## Conclusion

MoneyBin Sync's security model is:

**Better than** most financial services (encrypt at rest, user-controlled keys)
**Similar to** email services with PGP (process plaintext, encrypt for delivery)
**Not as good as** true zero-knowledge services (but they don't handle API integration)

**We chose this model because:**

1. It's the best practical option for automatic sync
2. It's significantly better than alternatives
3. It's honest about tradeoffs
4. Users can choose free tier for complete control

**We're transparent about:**

- What we can and can't protect against
- What requires trust
- What alternatives exist
- How to verify our claims
