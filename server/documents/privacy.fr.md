# Confidentialité Mochi

Cet avis de confidentialité explique comment **{{operator.name}}** (l'opérateur de ce serveur Mochi) collecte, utilise et partage vos données personnelles lorsque vous créez un compte ou utilisez le service. {{operator.name}} est le responsable du traitement.

## Ce que nous collectons

Lorsque vous vous inscrivez et utilisez ce serveur, nous collectons :

- **Données de compte** : un nom d'utilisateur, les méthodes d'authentification que vous choisissez (adresse e-mail, clés d'accès, application d'authentification, codes de récupération, ou un compte d'un fournisseur de connexion tiers), et toute information de profil que vous ajoutez (nom affiché, avatar, biographie).
- **Contenu que vous créez** : publications, messages, commentaires, fichiers et toute autre donnée que vous choisissez de publier via les applications installées sur ce serveur.
- **Contenu provenant d'autres personnes** : publications, messages et informations de profil de personnes sur d'autres serveurs Mochi et sur d'autres réseaux, qui parviennent à ce serveur parce que vous ou d'autres utilisateurs les suivez, vous abonnez à elles ou correspondez avec elles.
- **Signalements** : un signalement que vous faites au sujet de quelqu'un, ou que quelqu'un fait à votre sujet, sur Mochi ou depuis un autre réseau, est conservé afin que les administrateurs de ce serveur puissent y donner suite.
- **Données techniques** : adresse IP, informations sur le navigateur ou l'appareil, et horodatages des requêtes que vous effectuez. Ces données servent à maintenir le service sécurisé et opérationnel, et à détecter les abus.
- **Cookies** : un cookie de session est défini lors de votre connexion. Il ne stocke qu'un identifiant de session opaque ; nous n'utilisons pas de cookies à des fins publicitaires ou de suivi inter-sites.

Nous ne vendons pas vos données personnelles et nous ne les partageons pas avec des annonceurs.

## Comment Mochi gère vos données (c'est important)

Mochi est un **réseau pair à pair (P2P)**, et non une plateforme à serveur unique. Cela change la circulation de vos données d'une manière qui compte pour votre vie privée :

- Les contenus que vous publiez à l'attention de vos abonnés, suiveurs ou espaces partagés sont **répliqués vers d'autres serveurs Mochi** qui hébergent ces suiveurs ou abonnés. {{operator.name}} ne peut pas supprimer les copies de votre contenu sur les serveurs qu'il n'exploite pas. Si vous supprimez une publication, ce serveur enverra un avis de suppression aux pairs auxquels il a précédemment envoyé la publication, mais la suppression fonctionne au mieux et dépend de la coopération de ces pairs.
- Votre **identité (nom d'utilisateur, profil, clés publiques)** est publiée dans l'annuaire Mochi si vous choisissez un profil public, afin que les autres utilisateurs Mochi puissent vous trouver et vous contacter.
- Les contenus directs et privés (brouillons, notes privées, messages privés entre destinataires spécifiques) restent uniquement sur ce serveur et sur les serveurs des destinataires ; ils ne font pas partie du réseau P2P public.
- Lorsque ce serveur vous permet de **vous connecter à d'autres réseaux** - en liant un compte que vous détenez sur un autre service, ou en hébergeant ici un compte que des personnes d'autres réseaux peuvent suivre ou auquel elles peuvent envoyer des messages - ce que vous publiez ou envoyez via ce compte est transmis à des serveurs de ces réseaux. Ce ne sont pas des serveurs Mochi et {{operator.name}} ne les exploite pas. Un tel compte est public sur son réseau, et sur certains réseaux, les comptes que vous suivez, les publications que vous aimez et, sur Bluesky, les personnes que vous bloquez le sont aussi. Modifier ou supprimer quelque chose par la suite envoie une demande que ces serveurs peuvent ignorer.
- Les personnes avec qui vous discutez, sur Mochi et sur d'autres réseaux, peuvent voir quand vous êtes en ligne. Vous pouvez désactiver cela avec **Afficher quand je suis en ligne** dans les paramètres de l'application de messagerie.
- Un signalement que vous faites au sujet de quelqu'un sur un autre serveur peut aussi être transmis aux modérateurs de ce serveur-là, en tant que signalement de ce serveur-ci et non comme le vôtre : il n'indique pas qui l'a fait.

En résumé : tout ce que vous choisissez de partager avec d'autres personnes, sur Mochi ou sur d'autres réseaux, voyagera au-delà du contrôle de {{operator.name}}, par conception. Considérez le contenu que vous publiez comme vous le feriez pour un e-mail ou toute autre communication fédérée.

## Tiers

Nous partageons le minimum de données personnelles requis avec les tiers suivants lorsque vous utilisez les fonctionnalités associées :

- **Service de messagerie** : les e-mails sortants (codes de connexion, vérification d'adresse, notifications) sont envoyés via un fournisseur SMTP configuré par {{operator.name}}.
- **Fournisseurs de connexion tiers** : si vous vous connectez avec Google, GitHub, Microsoft, Facebook ou un autre fournisseur OAuth, ce fournisseur apprend que vous vous êtes connecté à ce serveur et nous communique votre adresse e-mail et vos données de profil de base.
- **Fournisseurs de paiement** : si vous effectuez un achat via une application de marketplace, le paiement est traité par Stripe (ou par tout autre fournisseur de paiement configuré par {{operator.name}}). Nous ne stockons pas les détails complets des cartes.
- **Services de notifications push** : les notifications destinées à l'application Mochi sur Android sont acheminées par Firebase Cloud Messaging de Google, qui transporte leur titre et leur texte. Si vous vous connectez à ce serveur avec une autre application, comme une application Mastodon, Matrix ou XMPP, ses notifications sont acheminées par le service push qu'utilise cette application et peuvent inclure le message auquel se rapporte une notification.
- **Autorité de certification** : lorsque ce serveur héberge des comptes Bluesky sous son propre domaine, un certificat de sécurité est délivré pour l'adresse de chaque compte. Les autorités de certification publient chaque certificat qu'elles délivrent dans des journaux publics, de sorte que ces adresses y figurent.
- **Autres réseaux** : si vous liez un compte que vous détenez sur un autre service, ce service reçoit ce que vous publiez par son intermédiaire, et ce serveur stocke l'identifiant qui lui permet d'y agir en votre nom. Si vous hébergez ici un compte pour un autre réseau, les serveurs et services de ce réseau reçoivent ce que vous y publiez, ainsi que l'adresse publique du compte.

## Vos droits

Selon votre lieu de résidence, vous pouvez disposer de droits d'accès, de rectification, d'exportation, de limitation ou de suppression de vos données personnelles. Vous pouvez exporter vos données et fermer votre compte vous-même dans Paramètres. La fermeture de votre compte le verrouille pendant un délai de grâce, durant lequel vous pouvez vous connecter et annuler la fermeture ; ensuite, le compte et ses données sont supprimés de ce serveur (sous réserve de la conservation des sauvegardes et de toute obligation légale de conserver des registres spécifiques). Pour exercer tout autre droit, ou si vous ne pouvez pas vous connecter, écrivez à **{{operator.email}}**.

## Conservation des données

Nous conservons les données de votre compte aussi longtemps que votre compte est actif, et pendant une période raisonnable après sa fermeture afin de pouvoir récupérer en cas de suppression accidentelle et satisfaire aux obligations légales et d'audit. Les sauvegardes sont généralement conservées jusqu'à 90 jours.

## Où nous nous trouvons

Ce service est exploité depuis **{{operator.jurisdiction}}**, et le droit de {{operator.jurisdiction}} s'applique au traitement de vos données personnelles sur ce serveur.

## Modifications

Nous pouvons mettre à jour ces conditions de temps en temps. Les changements importants seront annoncés avant leur entrée en vigueur. La poursuite de l'utilisation du service après une modification signifie que vous acceptez les conditions mises à jour.

## Contact

Pour toute question sur la confidentialité ou pour formuler une demande : **{{operator.email}}**
