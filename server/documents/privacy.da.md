# Mochi privatliv

Denne privatlivsmeddelelse forklarer, hvordan **{{operator.name}}** (operatøren af denne Mochi-server) indsamler, bruger og deler dine personoplysninger, når du opretter en konto eller bruger tjenesten. {{operator.name}} er dataansvarlig.

## Hvad vi indsamler

Når du tilmelder dig og bruger denne server, indsamler vi:

- **Kontooplysninger**: et brugernavn, de godkendelsesmetoder, du vælger (e-mailadresse, passkeys, godkendelses-app, gendannelseskoder eller en konto fra en tredjeparts loginudbyder), og eventuelle profiloplysninger, du tilføjer (visningsnavn, avatar, biografi).
- **Indhold, du opretter**: opslag, beskeder, kommentarer, filer og alle andre data, du vælger at offentliggøre via de apps, der er installeret på denne server.
- **Indhold fra andre personer**: opslag, beskeder og profiloplysninger om personer på andre Mochi-servere og på andre netværk, som når denne server, fordi du eller andre brugere følger dem, abonnerer på dem eller korresponderer med dem.
- **Tekniske oplysninger**: IP-adresse, browser-/enhedsoplysninger og tidsstempler for de anmodninger, du foretager. Disse bruges til at holde tjenesten sikker og kørende og til at opdage misbrug.
- **Cookies**: en sessionscookie sættes, når du logger ind. Den lagrer kun en uigennemsigtig sessionsidentifikator; vi bruger ikke cookies til reklame eller sporing på tværs af websteder.

Vi sælger ikke dine personoplysninger, og vi deler dem ikke med annoncører.

## Hvordan Mochi håndterer dine data (dette er vigtigt)

Mochi er et **peer-to-peer (P2P) netværk**, ikke en platform med en enkelt server. Det ændrer, hvordan dine data flyder, på måder der har betydning for dit privatliv:

- Indhold, du udgiver til følgere, abonnenter eller delte rum, **replikeres til andre Mochi-servere**, der er værter for disse følgere eller abonnenter. {{operator.name}} kan ikke slette kopier af dit indhold fra servere, som det ikke driver. Hvis du sletter et opslag, vil denne server sende en sletningsmeddelelse til de peers, den tidligere har sendt opslaget til, men sletningen er bedste indsats og afhænger af samarbejdet fra disse peers.
- Din **identitet (brugernavn, profil, offentlige nøgler)** offentliggøres i Mochi-mappen, hvis du vælger en offentlig profil, så andre Mochi-brugere kan finde og kontakte dig.
- Direkte, privat indhold (kladder, private noter, private beskeder mellem bestemte modtagere) forbliver kun på denne server og modtagernes servere; det er ikke en del af det offentlige P2P-netværk.
- Hvor denne server giver dig mulighed for at **oprette forbindelse til andre netværk** — ved at tilknytte en konto, du har hos en anden tjeneste, eller ved at være vært for en konto her, som personer på andre netværk kan følge eller sende beskeder til — går det, du udgiver eller sender via den konto, til servere på disse netværk. De er ikke Mochi-servere, og {{operator.name}} driver dem ikke. En sådan konto er offentlig på sit netværk, og på nogle netværk er de konti, du følger, og de opslag, du synes godt om, også offentlige. Redigering eller sletning af noget senere sender en anmodning, som disse servere kan ignorere.

Kort sagt: alt, hvad du vælger at dele med andre personer, på Mochi eller på andre netværk, vil bevæge sig ud over {{operator.name}}s kontrol — sådan er det designet. Behandl indhold, du udgiver, som du ville behandle e-mail eller anden fødereret kommunikation.

## Tredjeparter

Vi deler det minimum af personoplysninger, der kræves, med følgende tredjeparter, når du bruger de relaterede funktioner:

- **E-mailtjeneste**: udgående e-mails (loginkoder, adressebekræftelse, notifikationer) sendes via en SMTP-udbyder konfigureret af {{operator.name}}.
- **Tredjeparts loginudbydere**: hvis du logger ind med Google, GitHub, Microsoft, Facebook eller en anden OAuth-udbyder, får denne udbyder oplyst, at du har logget ind på denne server, og deler din e-mailadresse og grundlæggende profiloplysninger med os.
- **Betalingsudbydere**: hvis du foretager et køb via en marketplace-app, behandles betalingen af Stripe (eller den betalingsudbyder, som {{operator.name}} har konfigureret). Vi gemmer ikke fulde kortoplysninger.
- **Andre netværk**: hvis du tilknytter en konto, du har hos en anden tjeneste, modtager denne tjeneste det, du udgiver via den, og denne server gemmer de loginoplysninger, der gør det muligt for den at handle på dine vegne dér. Hvis du er vært for en konto her til et andet netværk, modtager dette netværks servere og tjenester det, du udgiver på det, sammen med kontoens offentlige adresse.

## Dine rettigheder

Afhængigt af hvor du bor, kan du have ret til at få adgang til, rette, eksportere, begrænse eller slette dine personoplysninger. Du kan selv eksportere dine data og lukke din konto i Indstillinger. Lukning af din konto låser den i en henstandsperiode, hvor du kan logge ind og annullere; derefter fjernes kontoen og dens data fra denne server (med forbehold for opbevaring af sikkerhedskopier og enhver retlig forpligtelse til at opbevare bestemte registreringer). For at udøve enhver anden rettighed, eller hvis du ikke kan logge ind, send en e-mail til **{{operator.email}}**.

## Opbevaring af data

Vi opbevarer dine kontooplysninger, så længe din konto er aktiv, og i en rimelig periode efter lukning for at kunne komme sig efter utilsigtet sletning og for at opfylde retlige og revisionsmæssige forpligtelser. Sikkerhedskopier opbevares typisk i op til 90 dage.

## Hvor vi er

Denne tjeneste drives fra **{{operator.jurisdiction}}**, og lovgivningen i {{operator.jurisdiction}} gælder for behandlingen af dine personoplysninger på denne server.

## Ændringer

Vi kan opdatere disse vilkår fra tid til anden. Væsentlige ændringer vil blive annonceret, før de træder i kraft. Fortsat brug af tjenesten efter en ændring betyder, at du accepterer de opdaterede vilkår.

## Kontakt

For ethvert privatlivsspørgsmål eller for at fremsætte en anmodning: **{{operator.email}}**
