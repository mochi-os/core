# Mochi Privacy

Deze privacyverklaring legt uit hoe **{{operator.name}}** (de beheerder van deze Mochi-server) uw persoonsgegevens verzamelt, gebruikt en deelt wanneer u een account aanmaakt of de dienst gebruikt. {{operator.name}} is de verwerkingsverantwoordelijke.

## Wat we verzamelen

Wanneer u zich aanmeldt en deze server gebruikt, verzamelen we:

- **Accountgegevens**: een gebruikersnaam, de authenticatiemethoden die u kiest (e-mailadres, passkeys, authenticator-app, herstelcodes of een account bij een externe inlogprovider), en alle profielinformatie die u toevoegt (weergavenaam, avatar, biografie).
- **Inhoud die u maakt**: berichten, mededelingen, reacties, bestanden en alle andere gegevens die u via de op deze server geïnstalleerde apps publiceert.
- **Inhoud van anderen**: berichten, mededelingen en profielgegevens van mensen op andere Mochi-servers en op andere netwerken, die deze server bereiken omdat u of andere gebruikers hen volgen, op hen geabonneerd zijn of met hen corresponderen.
- **Meldingen van overtredingen**: een melding die u over iemand doet, of die iemand over u doet, op Mochi of vanaf een ander netwerk, wordt bewaard zodat de beheerders van deze server er actie op kunnen ondernemen.
- **Technische gegevens**: IP-adres, browser- en apparaatinformatie en tijdstempels van uw verzoeken. Deze worden gebruikt om de dienst veilig en operationeel te houden en om misbruik op te sporen.
- **Cookies**: er wordt een sessiecookie geplaatst wanneer u zich aanmeldt. Deze bevat alleen een ondoorzichtige sessie-identificator; we gebruiken cookies niet voor reclame of cross-site tracking.

We verkopen uw persoonsgegevens niet en delen ze niet met adverteerders.

## Hoe Mochi met uw gegevens omgaat (dit is belangrijk)

Mochi is een **peer-to-peer (P2P) netwerk**, geen platform met één enkele server. Dat verandert hoe uw gegevens stromen, op manieren die voor uw privacy van belang zijn:

- Inhoud die u publiceert voor volgers, abonnees of gedeelde ruimtes wordt **gerepliceerd naar andere Mochi-servers** die deze volgers of abonnees hosten. {{operator.name}} kan kopieën van uw inhoud niet verwijderen van servers die het niet beheert. Wanneer u een bericht verwijdert, stuurt deze server een verwijderingsbericht naar de peers waarnaar het bericht eerder is gestuurd, maar de verwijdering is naar beste vermogen en hangt af van de medewerking van die peers.
- Uw **identiteit (gebruikersnaam, profiel, publieke sleutels)** wordt gepubliceerd in de Mochi-directory als u kiest voor een openbaar profiel, zodat andere Mochi-gebruikers u kunnen vinden en contact met u kunnen opnemen.
- Directe, privé-inhoud (kladversies, privé-notities, privéberichten tussen specifieke ontvangers) blijft alleen op deze server en op de servers van de ontvangers; deze maakt geen deel uit van het publieke P2P-netwerk.
- Waar deze server u in staat stelt **verbinding te maken met andere netwerken** - door een account dat u bij een andere dienst hebt te koppelen, of door hier een account te hosten dat mensen op andere netwerken kunnen volgen of berichten kunnen sturen - gaat wat u via dat account publiceert of verstuurt naar servers op die netwerken. Dat zijn geen Mochi-servers en {{operator.name}} beheert ze niet. Zo'n account is openbaar op zijn netwerk, en op sommige netwerken geldt dat ook voor de accounts die u volgt, de berichten die u leuk vindt en, op Bluesky, de mensen die u blokkeert. Als u later iets bewerkt of verwijdert, wordt een verzoek verstuurd dat die servers kunnen negeren.
- Mensen met wie u chat, op Mochi en op andere netwerken, kunnen zien wanneer u online bent. U kunt dit uitschakelen met **Tonen wanneer ik online ben** in de instellingen van de chat-app.
- Een melding die u doet over iemand op een andere server, kan ook naar de moderators van die server worden gestuurd, als melding van deze server en niet als uw melding: er staat niet in wie de melding heeft gedaan.

Kort gezegd: alles wat u kiest te delen met anderen, op Mochi of op andere netwerken, reist buiten de controle van {{operator.name}} - dat is bewust zo ontworpen. Behandel inhoud die u publiceert zoals u dat zou doen met e-mail of andere gefedereerde communicatie.

## Derden

We delen het minimum aan persoonsgegevens dat nodig is met de volgende derden wanneer u de bijbehorende functies gebruikt:

- **E-maildienst**: uitgaande e-mails (inlogcodes, adresverificatie, meldingen) worden verzonden via een door {{operator.name}} ingestelde SMTP-provider.
- **Externe inlogproviders**: als u inlogt met Google, GitHub, Microsoft, Facebook of een andere OAuth-provider, weet die provider dat u op deze server hebt ingelogd en deelt hij uw e-mailadres en basisprofielgegevens met ons.
- **Betalingsproviders**: als u een aankoop doet via een marktplaats-app, wordt de betaling verwerkt door Stripe (of welke betalingsprovider {{operator.name}} ook heeft ingesteld). We slaan geen volledige kaartgegevens op.
- **Pushmeldingsdiensten**: meldingen voor de Mochi-app op Android worden afgeleverd via Firebase Cloud Messaging van Google, dat de titel en tekst ervan doorgeeft. Als u zich met een andere app op deze server aanmeldt, zoals een Mastodon-, Matrix- of XMPP-app, worden de meldingen daarvan afgeleverd via de pushdienst die die app gebruikt, en kunnen ze het bericht bevatten waarover de melding gaat.
- **Certificaatautoriteit**: waar deze server Bluesky-accounts host onder zijn eigen domein, wordt voor het adres van elk account een beveiligingscertificaat uitgegeven. Certificaatautoriteiten publiceren elk certificaat dat ze uitgeven in openbare logboeken, waardoor die adressen daarin vermeld staan.
- **Andere netwerken**: als u een account koppelt dat u bij een andere dienst hebt, ontvangt die dienst wat u via dat account plaatst, en slaat deze server de inloggegevens op waarmee hij daar namens u kan handelen. Als u hier een account host voor een ander netwerk, ontvangen de servers en diensten van dat netwerk wat u erop publiceert, samen met het openbare adres van het account.

## Uw rechten

Afhankelijk van waar u woont, heeft u mogelijk het recht om uw persoonsgegevens in te zien, te corrigeren, te exporteren, te beperken of te verwijderen. U kunt uw gegevens zelf exporteren en uw account zelf sluiten in Instellingen. Als u uw account sluit, wordt het gedurende een respijtperiode vergrendeld, waarin u kunt inloggen en de sluiting kunt annuleren; daarna worden het account en de bijbehorende gegevens van deze server verwijderd (onder voorbehoud van back-upbewaring en eventuele wettelijke verplichtingen om bepaalde gegevens te bewaren). Om een ander recht uit te oefenen, of als u niet kunt inloggen, mailt u naar **{{operator.email}}**.

## Bewaartermijn van gegevens

We bewaren uw accountgegevens zolang uw account actief is, en gedurende een redelijke periode na sluiting om te kunnen herstellen van onbedoelde verwijdering en om te voldoen aan wettelijke en auditverplichtingen. Back-ups worden doorgaans tot 90 dagen bewaard.

## Waar we gevestigd zijn

Deze dienst wordt geëxploiteerd vanuit **{{operator.jurisdiction}}**, en op de verwerking van uw persoonsgegevens op deze server is het recht van {{operator.jurisdiction}} van toepassing.

## Wijzigingen

We kunnen deze voorwaarden van tijd tot tijd bijwerken. Belangrijke wijzigingen worden aangekondigd voordat ze van kracht worden. Voortgezet gebruik van de dienst na een wijziging betekent dat u de bijgewerkte voorwaarden aanvaardt.

## Contact

Voor privacyvragen of een verzoek: **{{operator.email}}**
