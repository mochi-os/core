# Mochin tietosuoja

Tämä tietosuojailmoitus selittää, miten **{{operator.name}}** (tämän Mochi-palvelimen operaattori) kerää, käyttää ja jakaa henkilötietojasi, kun luot tilin tai käytät palvelua. {{operator.name}} on rekisterinpitäjä.

## Mitä keräämme

Kun rekisteröidyt ja käytät tätä palvelinta, keräämme:

- **Tilitiedot**: käyttäjänimen, valitsemasi tunnistautumismenetelmät (sähköpostiosoitteen, passkeyt, todennussovelluksen, palautuskoodit tai kolmannen osapuolen kirjautumistarjoajan tilin) sekä lisäämäsi profiilitiedot (näyttönimi, profiilikuva, kuvaus).
- **Luomasi sisältö**: julkaisut, viestit, kommentit, tiedostot ja muut tiedot, jotka päätät julkaista palvelimelle asennettujen sovellusten kautta.
- **Muiden ihmisten sisältö**: muilla Mochi-palvelimilla ja muissa verkoissa olevien ihmisten julkaisut, viestit ja profiilitiedot, jotka saapuvat tälle palvelimelle, koska sinä tai muut käyttäjät seuraatte heitä, tilaatte heitä tai olette heidän kanssaan kirjeenvaihdossa.
- **Rikkomusilmoitukset**: ilmoitus, jonka teet jostakusta tai jonka joku tekee sinusta Mochissa tai toisesta verkosta, säilytetään, jotta tämän palvelimen ylläpitäjät voivat käsitellä sen.
- **Tekniset tiedot**: IP-osoite, selaimen ja laitteen tiedot sekä tekemiesi pyyntöjen aikaleimat. Näitä käytetään palvelun turvallisuuden ja toimivuuden ylläpitämiseen sekä väärinkäytön havaitsemiseen.
- **Evästeet**: kun kirjaudut sisään, asetetaan istuntoeväste. Se tallentaa vain läpinäkymättömän istuntotunnisteen; emme käytä evästeitä mainontaan tai sivustojen väliseen seurantaan.

Emme myy henkilötietojasi emmekä jaa niitä mainostajille.

## Miten Mochi käsittelee tietojasi (tämä on tärkeää)

Mochi on **vertaisverkko (P2P)**, ei yhden palvelimen alusta. Tämä muuttaa sitä, miten tietosi liikkuvat tavoilla, jotka ovat merkityksellisiä yksityisyytesi kannalta:

- Sisältö, jonka julkaiset seuraajille, tilaajille tai jaetuille tiloille, **replikoituu muille Mochi-palvelimille**, jotka isännöivät kyseisiä seuraajia tai tilaajia. {{operator.name}} ei voi poistaa sisältösi kopioita palvelimilta, joita se ei ylläpidä. Jos poistat julkaisun, tämä palvelin lähettää poistoilmoituksen niille vertaisille, joille julkaisu on aiemmin lähetetty, mutta poisto tapahtuu parhaan yrityksen periaatteella ja riippuu kyseisten vertaisten yhteistyöstä.
- **Identiteettisi (käyttäjänimi, profiili, julkiset avaimet)** julkaistaan Mochi-hakemistossa, jos valitset julkisen profiilin, jotta muut Mochi-käyttäjät voivat löytää sinut ja ottaa yhteyttä.
- Suora, yksityinen sisältö (luonnokset, yksityiset muistiinpanot, tiettyjen vastaanottajien väliset yksityiset viestit) pysyy vain tällä palvelimella ja vastaanottajien palvelimilla; se ei ole osa julkista P2P-verkkoa.
- Jos tämä palvelin antaa sinun **muodostaa yhteyden muihin verkkoihin** – linkittämällä tilin, joka sinulla on toisessa palvelussa, tai isännöimällä täällä tiliä, jota muiden verkkojen ihmiset voivat seurata tai jolle he voivat lähettää viestejä – se, mitä julkaiset tai lähetät tämän tilin kautta, menee näiden verkkojen palvelimille. Ne eivät ole Mochi-palvelimia, eikä {{operator.name}} ylläpidä niitä. Tällainen tili on julkinen omassa verkossaan, ja joissakin verkoissa myös seuraamasi tilit, tykkäämäsi julkaisut ja Blueskyssa estämäsi ihmiset ovat julkisia. Jonkin asian muokkaaminen tai poistaminen myöhemmin lähettää pyynnön, jonka nuo palvelimet voivat jättää huomiotta.
- Ihmiset, joiden kanssa keskustelet, Mochissa ja muissa verkoissa, näkevät, milloin olet verkossa. Voit poistaa tämän käytöstä keskustelusovelluksen asetuksissa valinnalla **Näytä, kun olen verkossa**.
- Ilmoitus, jonka teet jostakusta toisella palvelimella, voidaan lähettää myös kyseisen palvelimen moderaattoreille tämän palvelimen ilmoituksena eikä sinun: siinä ei kerrota, kuka sen teki.

Lyhyesti: kaikki, mitä päätät jakaa muiden ihmisten kanssa, Mochissa tai muissa verkoissa, kulkee {{operator.name}}n hallinnan ulkopuolelle, mikä on suunniteltua. Käsittele julkaisemaasi sisältöä kuten käsittelisit sähköpostia tai mitä tahansa muuta federoitua viestintää.

## Kolmannet osapuolet

Jaamme välttämättömät vähimmäishenkilötiedot seuraavien kolmansien osapuolten kanssa, kun käytät niihin liittyviä toimintoja:

- **Sähköpostipalvelu**: lähtevät sähköpostit (kirjautumiskoodit, osoitteen vahvistus, ilmoitukset) lähetetään {{operator.name}}n määrittämän SMTP-tarjoajan kautta.
- **Kolmansien osapuolten kirjautumistarjoajat**: jos kirjaudut sisään Googlen, GitHubin, Microsoftin, Facebookin tai jonkin muun OAuth-tarjoajan kautta, kyseinen tarjoaja saa tiedon siitä, että kirjauduit tälle palvelimelle, ja jakaa kanssamme sähköpostiosoitteesi ja perusprofiilitietosi.
- **Maksupalveluntarjoajat**: jos teet ostoksen markkinapaikkasovelluksessa, maksun käsittelee Stripe (tai mikä tahansa maksupalveluntarjoaja, jonka {{operator.name}} on määrittänyt). Emme tallenna täydellisiä maksukortin tietoja.
- **Push-ilmoituspalvelut**: Mochi-sovellukseen Androidilla tulevat ilmoitukset toimitetaan Googlen Firebase Cloud Messaging -palvelun kautta, joka välittää niiden otsikon ja tekstin. Jos kirjaudut tälle palvelimelle jollakin toisella sovelluksella, kuten Mastodon-, Matrix- tai XMPP-sovelluksella, sen ilmoitukset toimitetaan sen push-palvelun kautta, jota kyseinen sovellus käyttää, ja ne voivat sisältää viestin, jota ilmoitus koskee.
- **Varmenteiden myöntäjä**: kun tämä palvelin isännöi Bluesky-tilejä omalla verkkotunnuksellaan, jokaisen tilin osoitteelle myönnetään suojausvarmenne. Varmenteiden myöntäjät julkaisevat jokaisen myöntämänsä varmenteen julkisissa lokeissa, joten nuo osoitteet on lueteltu niissä.
- **Muut verkot**: jos linkität tilin, joka sinulla on toisessa palvelussa, kyseinen palvelu vastaanottaa sen, mitä julkaiset sen kautta, ja tämä palvelin tallentaa tunnistetiedon, jonka avulla se voi toimia puolestasi siellä. Jos isännöit täällä tiliä toista verkkoa varten, kyseisen verkon palvelimet ja palvelut vastaanottavat sen, mitä julkaiset siihen, sekä tilin julkisen osoitteen.

## Oikeutesi

Asuinpaikastasi riippuen sinulla voi olla oikeus tarkastaa, oikaista, viedä, rajoittaa tai poistaa henkilötietojasi. Voit itse viedä tietosi ja sulkea tilisi Asetuksissa. Tilin sulkeminen lukitsee sen armonajaksi, jonka aikana voit kirjautua sisään ja perua sulkemisen; sen jälkeen tili ja sen tiedot poistetaan tältä palvelimelta (varmuuskopioiden säilytyksen ja tiettyjen tietueiden säilyttämistä koskevien oikeudellisten velvoitteiden rajoissa). Käyttääksesi mitä tahansa muuta oikeutta tai jos et pysty kirjautumaan sisään, lähetä sähköpostia osoitteeseen **{{operator.email}}**.

## Tietojen säilyttäminen

Säilytämme tilitietojasi niin kauan kuin tilisi on aktiivinen, ja kohtuullisen ajan sulkemisen jälkeen toipuaksemme vahingossa tapahtuneista poistoista sekä täyttääksemme oikeudelliset ja tarkastusvelvollisuutemme. Varmuuskopioita säilytetään tyypillisesti enintään 90 päivää.

## Missä olemme

Tätä palvelua operoidaan maasta **{{operator.jurisdiction}}**, ja {{operator.jurisdiction}}n lakia sovelletaan henkilötietojesi käsittelyyn tällä palvelimella.

## Muutokset

Voimme päivittää näitä ehtoja ajoittain. Olennaisista muutoksista ilmoitetaan ennen niiden voimaantuloa. Palvelun käytön jatkaminen muutoksen jälkeen tarkoittaa, että hyväksyt päivitetyt ehdot.

## Yhteystiedot

Tietosuojakysymyksiä tai pyyntöjä varten: **{{operator.email}}**
