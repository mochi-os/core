# Mochi privatnost

Ova obavijest o privatnosti objašnjava kako **{{operator.name}}** (operator ovog Mochi poslužitelja) prikuplja, koristi i dijeli vaše osobne podatke kada izradite račun ili koristite uslugu. {{operator.name}} je voditelj obrade podataka.

## Što prikupljamo

Kada se registrirate i koristite ovaj poslužitelj prikupljamo:

- **Podatke o računu**: korisničko ime, metode autentifikacije koje odaberete (adresa e-pošte, passkey, autentifikacijska aplikacija, kodovi za oporavak ili račun kod davatelja prijave treće strane) i sve podatke profila koje dodate (prikazno ime, avatar, biografija).
- **Sadržaj koji izrađujete**: objave, poruke, komentari, datoteke i svi drugi podaci koje odlučite objaviti putem aplikacija instaliranih na ovom poslužitelju.
- **Sadržaj drugih ljudi**: objave, poruke i podaci profila ljudi na drugim Mochi poslužiteljima i na drugim mrežama, koji dospijevaju na ovaj poslužitelj zato što ih vi ili drugi korisnici pratite, na njih ste pretplaćeni ili se s njima dopisujete.
- **Prijave**: prijava koju podnesete o nekome, ili koju netko podnese o vama, na Mochiju ili s druge mreže, čuva se kako bi administratori ovog poslužitelja mogli postupiti po njoj.
- **Tehničke podatke**: IP adresa, podaci o pregledniku/uređaju i vremenske oznake za zahtjeve koje upućujete. Ovo se koristi kako bi usluga ostala sigurna i operativna te radi otkrivanja zlouporabe.
- **Kolačiće**: kolačić sjednice postavlja se kada se prijavite. Pohranjuje samo neproziran identifikator sjednice; ne koristimo kolačiće za oglašavanje ni za praćenje između web-mjesta.

Ne prodajemo vaše osobne podatke i ne dijelimo ih s oglašivačima.

## Kako Mochi postupa s vašim podacima (ovo je važno)

Mochi je **peer-to-peer (P2P) mreža**, a ne platforma s jednim poslužiteljem. To mijenja način na koji se vaši podaci kreću na načine koji su važni za vašu privatnost:

- Sadržaj koji objavite pratiteljima, pretplatnicima ili u dijeljenim prostorima **replicira se na druge Mochi poslužitelje** koji ugošćuju te pratitelje ili pretplatnike. {{operator.name}} ne može izbrisati kopije vašeg sadržaja s poslužitelja kojima ne upravlja. Ako izbrišete objavu, ovaj poslužitelj poslat će obavijest o brisanju peerovima kojima je objava prethodno bila poslana, ali brisanje je tek najbolji mogući napor i ovisi o suradnji tih peerova.
- Vaš **identitet (korisničko ime, profil, javni ključevi)** objavljuje se u Mochi imeniku ako odaberete javni profil, kako bi vas drugi Mochi korisnici mogli pronaći i kontaktirati.
- Izravan, privatan sadržaj (skice, privatne bilješke, privatne poruke između određenih primatelja) ostaje samo na ovom poslužitelju i poslužiteljima primatelja; nije dio javne P2P mreže.
- Ondje gdje vam ovaj poslužitelj omogućuje **povezivanje s drugim mrežama** — povezivanjem računa koji imate na drugoj usluzi ili ugošćivanjem računa ovdje koji ljudi na drugim mrežama mogu pratiti ili mu slati poruke — ono što objavite ili pošaljete putem tog računa odlazi na poslužitelje tih mreža. To nisu Mochi poslužitelji i {{operator.name}} njima ne upravlja. Takav je račun javan na svojoj mreži, a na nekim mrežama javni su i računi koje pratite, objave koje vam se sviđaju te, na Blueskyju, osobe koje blokirate. Naknadno uređivanje ili brisanje nečega šalje zahtjev koji ti poslužitelji mogu zanemariti.
- Osobe s kojima razgovarate, na Mochiju i na drugim mrežama, mogu vidjeti kada ste na mreži. To možete isključiti opcijom **Prikaži kad sam na mreži** u postavkama aplikacije za razgovor.
- Prijava koju podnesete o nekome na drugom poslužitelju može biti poslana i moderatorima tog poslužitelja, i to kao prijava ovog poslužitelja, a ne vaša: u njoj se ne navodi tko ju je podnio.

Ukratko: sve što odlučite podijeliti s drugim ljudima, na Mochiju ili na drugim mrežama, izaći će izvan kontrole {{operator.name}} po samoj zamisli sustava. Sadržaj koji objavljujete tretirajte onako kako biste tretirali e-poštu ili bilo koju drugu federiranu komunikaciju.

## Treće strane

Sa sljedećim trećim stranama dijelimo minimum potrebnih osobnih podataka kada koristite povezane značajke:

- **Usluga e-pošte**: odlazna e-pošta (kodovi za prijavu, provjera adrese, obavijesti) šalje se putem SMTP davatelja kojeg je konfigurirao {{operator.name}}.
- **Davatelji prijave treće strane**: ako se prijavite putem Googlea, GitHuba, Microsofta, Facebooka ili drugog OAuth davatelja, taj davatelj saznaje da ste se prijavili na ovaj poslužitelj te s nama dijeli vašu adresu e-pošte i osnovne podatke profila.
- **Davatelji platnih usluga**: ako kupujete putem aplikacije tržnice, plaćanje obrađuje Stripe (ili koji god davatelj platnih usluga koje je {{operator.name}} konfigurirao). Ne pohranjujemo cjelovite podatke o kartici.
- **Usluge push obavijesti**: obavijesti Mochi aplikaciji na Androidu isporučuju se putem Googleove usluge Firebase Cloud Messaging, koja prenosi njihov naslov i tekst. Ako se na ovaj poslužitelj prijavite drugom aplikacijom, primjerice Mastodon, Matrix ili XMPP aplikacijom, njezine se obavijesti isporučuju putem push usluge koju ta aplikacija koristi i mogu sadržavati poruku na koju se obavijest odnosi.
- **Tijelo za izdavanje certifikata**: ondje gdje ovaj poslužitelj ugošćuje Bluesky račune pod vlastitom domenom, za adresu svakog računa izdaje se sigurnosni certifikat. Tijela za izdavanje certifikata objavljuju svaki certifikat koji izdaju u javnim zapisnicima, pa su te adrese ondje navedene.
- **Druge mreže**: ako povežete račun koji imate na drugoj usluzi, ta usluga prima ono što putem njega objavite, a ovaj poslužitelj pohranjuje vjerodajnicu koja mu omogućuje da ondje djeluje u vaše ime. Ako ovdje ugošćujete račun za drugu mrežu, poslužitelji i usluge te mreže primaju ono što na nju objavite, zajedno s javnom adresom računa.

## Vaša prava

Ovisno o tome gdje živite, možete imati pravo pristupa, ispravka, izvoza, ograničenja ili brisanja svojih osobnih podataka. Svoje podatke možete sami izvesti i zatvoriti račun u Postavkama. Zatvaranje računa zaključava ga na razdoblje odgode, tijekom kojeg se možete prijaviti i odustati; nakon toga račun i njegovi podaci uklanjaju se s ovog poslužitelja (uz iznimku zadržavanja sigurnosnih kopija i bilo koje zakonske obveze čuvanja određenih zapisa). Kako biste ostvarili bilo koje drugo pravo, ili ako se ne možete prijaviti, pošaljite e-poštu na **{{operator.email}}**.

## Čuvanje podataka

Podatke o računu čuvamo dok je vaš račun aktivan i razumno razdoblje nakon zatvaranja kako bismo se mogli oporaviti od slučajnog brisanja te ispuniti zakonske i revizijske obveze. Sigurnosne kopije obično se zadržavaju do 90 dana.

## Gdje se nalazimo

Ova usluga djeluje iz **{{operator.jurisdiction}}**, a na obradu vaših osobnih podataka na ovom poslužitelju primjenjuje se pravo {{operator.jurisdiction}}.

## Izmjene

Ove uvjete možemo s vremena na vrijeme ažurirati. Bitne izmjene bit će najavljene prije nego što stupe na snagu. Nastavak korištenja usluge nakon izmjene znači da prihvaćate ažurirane uvjete.

## Kontakt

Za bilo koje pitanje o privatnosti ili podnošenje zahtjeva: **{{operator.email}}**
