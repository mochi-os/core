# Mochi-Datenschutz

Diese Datenschutzerklärung erläutert, wie **{{operator.name}}** (der Betreiber dieses Mochi-Servers) Ihre personenbezogenen Daten erfasst, verwendet und weitergibt, wenn Sie ein Konto erstellen oder den Dienst nutzen. {{operator.name}} ist der für die Verarbeitung Verantwortliche.

## Was wir erfassen

Wenn Sie sich registrieren und diesen Server nutzen, erfassen wir:

- **Kontodaten**: einen Benutzernamen, die von Ihnen gewählten Authentifizierungsmethoden (E-Mail-Adresse, Passkeys, Authentifizierungs-App, Wiederherstellungscodes oder ein Konto eines Drittanbieter-Anmeldedienstes) sowie alle Profilinformationen, die Sie hinzufügen (Anzeigename, Avatar, Biografie).
- **Inhalte, die Sie erstellen**: Beiträge, Nachrichten, Kommentare, Dateien und alle anderen Daten, die Sie über die auf diesem Server installierten Apps veröffentlichen.
- **Inhalte anderer Personen**: Beiträge, Nachrichten und Profilangaben von Personen auf anderen Mochi-Servern und in anderen Netzwerken, die diesen Server erreichen, weil Sie oder andere Nutzer ihnen folgen, sie abonnieren oder mit ihnen korrespondieren.
- **Meldungen**: Eine Meldung, die Sie zu jemandem einreichen oder die jemand zu Ihnen einreicht, auf Mochi oder aus einem anderen Netzwerk, wird aufbewahrt, damit die Administratoren dieses Servers darauf reagieren können.
- **Technische Daten**: IP-Adresse, Browser-/Geräteinformationen und Zeitstempel der von Ihnen gestellten Anfragen. Diese werden verwendet, um den Dienst sicher und betriebsbereit zu halten und Missbrauch zu erkennen.
- **Cookies**: Beim Anmelden wird ein Sitzungs-Cookie gesetzt. Es speichert nur eine undurchsichtige Sitzungskennung; wir verwenden Cookies weder für Werbung noch für seitenübergreifendes Tracking.

Wir verkaufen Ihre personenbezogenen Daten nicht und geben sie nicht an Werbetreibende weiter.

## Wie Mochi mit Ihren Daten umgeht (das ist wichtig)

Mochi ist ein **Peer-to-Peer-Netzwerk (P2P)**, keine Plattform mit nur einem Server. Das ändert die Art und Weise, wie Ihre Daten fließen, in einer Weise, die für Ihre Privatsphäre relevant ist:

- Inhalte, die Sie für Follower, Abonnenten oder gemeinsam genutzte Bereiche veröffentlichen, werden zu **anderen Mochi-Servern repliziert**, die diese Follower oder Abonnenten hosten. {{operator.name}} kann Kopien Ihrer Inhalte nicht von Servern löschen, die nicht von ihm betrieben werden. Wenn Sie einen Beitrag löschen, sendet dieser Server eine Löschmitteilung an die Peers, an die er den Beitrag zuvor gesendet hat. Die Löschung erfolgt jedoch nach bestem Bemühen und ist von der Mitwirkung dieser Peers abhängig.
- Ihre **Identität (Benutzername, Profil, öffentliche Schlüssel)** wird im Mochi-Verzeichnis veröffentlicht, wenn Sie ein öffentliches Profil wählen, damit andere Mochi-Nutzer Sie finden und kontaktieren können.
- Direkte, private Inhalte (Entwürfe, private Notizen, private Nachrichten zwischen bestimmten Empfängern) verbleiben nur auf diesem Server und auf den Servern der Empfänger; sie sind nicht Teil des öffentlichen P2P-Netzwerks.
- Wo dieser Server es Ihnen ermöglicht, **sich mit anderen Netzwerken zu verbinden** – indem Sie ein Konto verknüpfen, das Sie bei einem anderen Dienst haben, oder indem Sie hier ein Konto hosten, dem Personen in anderen Netzwerken folgen oder Nachrichten senden können –, geht das, was Sie über dieses Konto veröffentlichen oder senden, an Server in diesen Netzwerken. Es handelt sich nicht um Mochi-Server, und {{operator.name}} betreibt sie nicht. Ein solches Konto ist in seinem Netzwerk öffentlich, und in manchen Netzwerken gilt das auch für die Konten, denen Sie folgen, die Beiträge, die Ihnen gefallen, und, auf Bluesky, die Personen, die Sie blockieren. Wenn Sie später etwas bearbeiten oder löschen, wird eine Anfrage gesendet, die diese Server ignorieren können.
- Personen, mit denen Sie chatten, auf Mochi und in anderen Netzwerken, können sehen, wann Sie online sind. Sie können das mit **Anzeigen, wann ich online bin** in den Einstellungen der Chat-App ausschalten.
- Eine Meldung, die Sie zu jemandem auf einem anderen Server einreichen, kann auch an die Moderatoren jenes Servers gesendet werden, als Meldung dieses Servers und nicht als Ihre: sie gibt nicht an, wer sie eingereicht hat.

Kurz gesagt: Alles, was Sie sich entscheiden, mit anderen Personen, auf Mochi oder in anderen Netzwerken, zu teilen, wird über die Kontrolle von {{operator.name}} hinausgehen, und das ist Teil des Designs. Behandeln Sie veröffentlichte Inhalte so, wie Sie E-Mails oder jede andere föderierte Kommunikation behandeln würden.

## Dritte

Wir geben die mindestens erforderlichen personenbezogenen Daten an die folgenden Dritten weiter, wenn Sie die zugehörigen Funktionen nutzen:

- **E-Mail-Dienst**: ausgehende E-Mails (Anmeldecodes, Adressbestätigung, Benachrichtigungen) werden über einen von {{operator.name}} konfigurierten SMTP-Anbieter versendet.
- **Drittanbieter-Anmeldedienste**: Wenn Sie sich mit Google, GitHub, Microsoft, Facebook oder einem anderen OAuth-Anbieter anmelden, erfährt dieser Anbieter, dass Sie sich auf diesem Server angemeldet haben, und teilt Ihre E-Mail-Adresse sowie grundlegende Profildaten mit uns.
- **Zahlungsanbieter**: Wenn Sie einen Kauf über eine Marktplatz-App tätigen, wird die Zahlung von Stripe (oder einem anderen von {{operator.name}} konfigurierten Zahlungsanbieter) abgewickelt. Wir speichern keine vollständigen Kartendaten.
- **Push-Benachrichtigungsdienste**: Benachrichtigungen an die Mochi-App unter Android werden über Firebase Cloud Messaging von Google zugestellt, das ihren Titel und Text überträgt. Wenn Sie sich mit einer anderen App auf diesem Server anmelden, etwa einer Mastodon-, Matrix- oder XMPP-App, werden deren Benachrichtigungen über den Push-Dienst zugestellt, den diese App verwendet, und können die Nachricht enthalten, um die es in einer Benachrichtigung geht.
- **Zertifizierungsstelle**: Wo dieser Server Bluesky-Konten unter seiner eigenen Domain hostet, wird für die Adresse jedes Kontos ein Sicherheitszertifikat ausgestellt. Zertifizierungsstellen veröffentlichen jedes von ihnen ausgestellte Zertifikat in öffentlichen Protokollen, sodass diese Adressen dort aufgeführt sind.
- **Andere Netzwerke**: Wenn Sie ein Konto verknüpfen, das Sie bei einem anderen Dienst haben, erhält dieser Dienst, was Sie darüber veröffentlichen, und dieser Server speichert die Anmeldedaten, mit denen er dort in Ihrem Namen handeln kann. Wenn Sie hier ein Konto für ein anderes Netzwerk hosten, erhalten die Server und Dienste dieses Netzwerks, was Sie dort veröffentlichen, zusammen mit der öffentlichen Adresse des Kontos.

## Ihre Rechte

Je nach Ihrem Wohnort haben Sie möglicherweise das Recht, Ihre personenbezogenen Daten einzusehen, zu korrigieren, zu exportieren, deren Verarbeitung einzuschränken oder zu löschen. In den Einstellungen können Sie Ihre Daten selbst exportieren und Ihr Konto selbst schließen. Die Schließung Ihres Kontos sperrt es für eine Karenzzeit, während der Sie sich anmelden und die Schließung abbrechen können; danach werden das Konto und seine Daten von diesem Server entfernt (vorbehaltlich der Aufbewahrung in Sicherungskopien und etwaiger gesetzlicher Aufbewahrungspflichten für bestimmte Aufzeichnungen). Um ein anderes Recht auszuüben oder wenn Sie sich nicht anmelden können, senden Sie eine E-Mail an **{{operator.email}}**.

## Aufbewahrung von Daten

Wir bewahren Ihre Kontodaten so lange auf, wie Ihr Konto aktiv ist, und für einen angemessenen Zeitraum nach der Schließung, um eine Wiederherstellung nach versehentlichem Löschen zu ermöglichen sowie gesetzliche und Auditpflichten zu erfüllen. Sicherungskopien werden in der Regel bis zu 90 Tage aufbewahrt.

## Wo wir uns befinden

Dieser Dienst wird aus **{{operator.jurisdiction}}** betrieben, und das Recht von {{operator.jurisdiction}} gilt für die Verarbeitung Ihrer personenbezogenen Daten auf diesem Server.

## Änderungen

Wir können diese Bedingungen von Zeit zu Zeit aktualisieren. Wesentliche Änderungen werden vor ihrem Inkrafttreten angekündigt. Die fortgesetzte Nutzung des Dienstes nach einer Änderung bedeutet, dass Sie die aktualisierten Bedingungen akzeptieren.

## Kontakt

Bei Fragen zum Datenschutz oder für eine Anfrage: **{{operator.email}}**
