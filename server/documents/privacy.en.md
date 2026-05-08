# Mochi Privacy

This privacy notice explains how **{{operator.name}}** (the operator of this Mochi server) collects, uses, and shares your personal data when you create an account or use the service. {{operator.name}} is the data controller.

## What we collect

When you sign up and use this server we collect:

- **Account data**: a username, the authentication methods you choose (email address, passkeys, authenticator app, recovery codes, or an account from a third-party login provider), and any profile information you add (display name, avatar, biography).
- **Content you create**: posts, messages, comments, files, and any other data you choose to publish through the apps installed on this server.
- **Technical data**: IP address, browser/device information, and timestamps for the requests you make. This is used to keep the service secure and operating, and to detect abuse.
- **Cookies**: a session cookie is set when you sign in. It stores only an opaque session identifier; we do not use cookies for advertising or cross-site tracking.

We do not sell your personal data, and we do not share it with advertisers.

## How Mochi handles your data (this is important)

Mochi is a **peer-to-peer (P2P) network**, not a single-server platform. That changes how your data flows in ways that matter for your privacy:

- Content you publish to followers, subscribers, or shared spaces is **replicated to other Mochi servers** that host those followers or subscribers. {{operator.name}} cannot delete copies of your content from servers it does not run. If you delete a post, this server will send a deletion notice to the peers it has previously sent the post to, but the deletion is best-effort and depends on the cooperation of those peers.
- Your **identity (username, profile, public keys)** is published to the Mochi directory if you choose a public profile, so other Mochi users can find and contact you.
- Direct, private content (drafts, private notes, private messages between specific recipients) stays on this server and the recipients' servers only; it is not part of the public P2P network.

In short: anything you choose to share with other people on Mochi will travel beyond {{operator.name}}'s control, by design. Treat content you publish as you would email or any other federated communication.

## Third parties

We share the minimum personal data required with the following third parties when you use the related features:

- **Email service**: outbound emails (sign-up codes, notifications, password resets) are sent through an SMTP provider configured by {{operator.name}}.
- **Third-party login providers**: if you sign in with Google, GitHub, Microsoft, Facebook, or another OAuth provider, that provider learns that you signed in to this server and shares your email address and basic profile data with us.
- **Payment providers**: if you make a purchase through a marketplace app, the payment is processed by Stripe (or whichever payment provider {{operator.name}} has configured). We do not store full card details.

## Your rights

Depending on where you live, you may have rights to access, correct, export, restrict, or delete your personal data. To exercise any of these rights - including closing your account and having your data removed from this server - email **{{operator.email}}**. Account closure is currently handled by the server operator on request; once we receive your email we will close the account and remove the associated data from this server (subject to backup retention and any legal obligation to retain specific records).

## Data retention

We keep your account data for as long as your account is active, and for a reasonable period after closure to recover from accidental deletion and to satisfy legal and audit obligations. Backups are typically retained for up to 90 days.

## Where we are

This service is operated from **{{operator.jurisdiction}}**, and the law of {{operator.jurisdiction}} applies to the processing of your personal data on this server.

## Changes

We may update these terms from time to time. Material changes will be announced before they take effect. Continued use of the service after a change means you accept the updated terms.

## Contact

For any privacy question or to make a request: **{{operator.email}}**
