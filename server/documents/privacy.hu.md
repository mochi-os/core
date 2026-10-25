# A Mochi adatvédelme

Ez az adatvédelmi tájékoztató ismerteti, hogyan gyűjti, használja és osztja meg személyes adataidat a **{{operator.name}}** (e Mochi szerver üzemeltetője), amikor fiókot hozol létre vagy használod a szolgáltatást. A {{operator.name}} az adatkezelő.

## Mit gyűjtünk

A regisztrációkor és a szerver használata során a következőket gyűjtjük:

- **Fiókadatok**: felhasználónév, az általad választott hitelesítési módszerek (e-mail-cím, passkey-ek, hitelesítő alkalmazás, helyreállítási kódok vagy harmadik féltől származó belépési szolgáltató fiókja), valamint bármely megadott profilinformáció (megjelenítendő név, profilkép, életrajz).
- **Az általad létrehozott tartalom**: bejegyzések, üzenetek, hozzászólások, fájlok és minden egyéb adat, amelyet a szerveren telepített alkalmazásokon keresztül közzéteszel.
- **Mások tartalma**: más Mochi szervereken és más hálózatokon lévő emberek bejegyzései, üzenetei és profiladatai, amelyek azért érkeznek erre a szerverre, mert te vagy más felhasználók követitek őket, feliratkoztok rájuk vagy leveleztek velük.
- **Bejelentések**: az a bejelentés, amelyet te teszel valakiről, vagy amelyet valaki rólad tesz, a Mochin vagy egy másik hálózatról, megőrzésre kerül, hogy e szerver adminisztrátorai intézkedhessenek az ügyben.
- **Technikai adatok**: IP-cím, böngésző- és eszközinformációk, valamint a kérések időbélyegei. Ezt a szolgáltatás biztonságának és működésének fenntartására, valamint visszaélések felderítésére használjuk.
- **Sütik**: bejelentkezéskor egy munkamenet-süti kerül beállításra. Ez csak egy átlátszatlan munkamenet-azonosítót tárol; nem használunk sütit reklámcélokra vagy keresztoldalas követésre.

Nem értékesítjük személyes adataidat, és nem osztjuk meg azokat hirdetőkkel.

## Hogyan kezeli a Mochi az adataidat (ez fontos)

A Mochi egy **peer-to-peer (P2P) hálózat**, nem egyetlen szerveres platform. Ez olyan módokon befolyásolja az adataid áramlását, amelyek számítanak a magánéleted szempontjából:

- A követőidnek, feliratkozóidnak vagy megosztott terekbe közzétett tartalom **más Mochi szerverekre is replikálódik**, amelyek ezeket a követőket vagy feliratkozókat tárolják. A {{operator.name}} nem tudja törölni a tartalmadról készült másolatokat olyan szerverekről, amelyeket nem ő üzemeltet. Ha törölsz egy bejegyzést, ez a szerver törlési értesítést küld azoknak a társgépeknek, amelyeknek korábban elküldte a bejegyzést, de a törlés legjobb erőfeszítésen alapul, és e társgépek együttműködésétől függ.
- Az **azonosságod (felhasználónév, profil, nyilvános kulcsok)** közzétételre kerül a Mochi névjegyzékben, ha nyilvános profilt választasz, így más Mochi felhasználók megtalálhatnak és kapcsolatba léphetnek veled.
- A közvetlen, privát tartalom (vázlatok, privát jegyzetek, meghatározott címzettek közötti privát üzenetek) csak ezen a szerveren és a címzettek szerverein marad; nem része a nyilvános P2P hálózatnak.
- Ahol ez a szerver lehetővé teszi, hogy **más hálózatokhoz kapcsolódj** – egy másik szolgáltatásnál meglévő fiókod összekapcsolásával, vagy egy itt tárolt olyan fiókkal, amelyet más hálózatokon lévő emberek követhetnek, vagy amelynek üzenetet küldhetnek –, ott amit ezen a fiókon keresztül közzéteszel vagy elküldesz, az e hálózatok szervereire kerül. Ezek nem Mochi szerverek, és nem a {{operator.name}} üzemelteti őket. Egy ilyen fiók nyilvános a saját hálózatán, és egyes hálózatokon az általad követett fiókok, az általad kedvelt bejegyzések, a Blueskyon pedig az általad letiltott személyek is nyilvánosak. Ha később szerkesztesz vagy törölsz valamit, az egy kérést küld, amelyet ezek a szerverek figyelmen kívül hagyhatnak.
- Azok, akikkel csevegsz, a Mochin és más hálózatokon, láthatják, mikor vagy elérhető. Ezt a csevegőalkalmazás beállításaiban a **Mutassa, ha elérhető vagyok** lehetőséggel kapcsolhatod ki.
- Ha egy másik szerveren lévő személyről teszel bejelentést, az annak a szervernek a moderátoraihoz is eljuthat, mégpedig e szerver bejelentéseként, nem a tiédként: nem derül ki belőle, ki tette.

Röviden: bármi, amit úgy döntesz, hogy megosztasz másokkal, a Mochin vagy más hálózatokon, tervezésénél fogva túljut a {{operator.name}} ellenőrzésén. Úgy kezeld a közzétett tartalmat, ahogyan az e-mailt vagy bármely más föderált kommunikációt kezelnéd.

## Harmadik felek

A kapcsolódó funkciók használatakor a szükséges minimális személyes adatot megosztjuk az alábbi harmadik felekkel:

- **E-mail-szolgáltatás**: a kimenő e-maileket (bejelentkezési kódok, címellenőrzés, értesítések) a {{operator.name}} által beállított SMTP szolgáltatón keresztül küldjük.
- **Harmadik féltől származó belépési szolgáltatók**: ha Google, GitHub, Microsoft, Facebook vagy más OAuth szolgáltatóval lépsz be, az adott szolgáltató tudomást szerez arról, hogy beléptél erre a szerverre, és megosztja velünk az e-mail-címedet és az alapvető profiladatokat.
- **Fizetési szolgáltatók**: ha vásárlást végzel egy piactér-alkalmazáson keresztül, a fizetést a Stripe (vagy bármely, a {{operator.name}} által beállított fizetési szolgáltató) dolgozza fel. Nem tároljuk a teljes kártyaadatokat.
- **Leküldéses értesítési szolgáltatások**: az Androidon futó Mochi alkalmazásnak szóló értesítéseket a Google Firebase Cloud Messaging szolgáltatása kézbesíti, amely az értesítések címét és szövegét is továbbítja. Ha más alkalmazással, például Mastodon-, Matrix- vagy XMPP-alkalmazással lépsz be erre a szerverre, annak értesítéseit az adott alkalmazás által használt leküldéses szolgáltatás kézbesíti, és ezek tartalmazhatják azt az üzenetet, amelyre az értesítés vonatkozik.
- **Tanúsítványkiadó**: ahol ez a szerver a saját domainje alatt tárol Bluesky-fiókokat, minden fiók címére biztonsági tanúsítványt állítanak ki. A tanúsítványkiadók minden általuk kiállított tanúsítványt nyilvános naplókban tesznek közzé, így ezek a címek ott szerepelnek.
- **Más hálózatok**: ha összekapcsolsz egy másik szolgáltatásnál meglévő fiókodat, az a szolgáltatás megkapja, amit rajta keresztül közzéteszel, ez a szerver pedig tárolja azt a hitelesítő adatot, amellyel ott a nevedben eljárhat. Ha itt egy másik hálózat számára tartasz fenn fiókot, annak a hálózatnak a szerverei és szolgáltatásai megkapják, amit oda közzéteszel, a fiók nyilvános címével együtt.

## A jogaid

Attól függően, hol élsz, jogod lehet hozzáférni személyes adataidhoz, helyesbíteni, exportálni, korlátozni vagy törölni azokat. Az adataidat magad is exportálhatod, és a fiókodat is magad szüntetheted meg a Beállításokban. A fiók megszüntetése egy türelmi időre zárolja a fiókot, amely alatt bejelentkezhetsz és visszavonhatod; ezt követően a fiók és az adatai eltávolításra kerülnek erről a szerverről (a biztonsági mentések megőrzésére és bármely meghatározott nyilvántartás megőrzésére vonatkozó jogi kötelezettségre figyelemmel). Bármely más jog gyakorlásához, vagy ha nem tudsz bejelentkezni, írj a **{{operator.email}}** címre.

## Adatmegőrzés

A fiókadatokat addig őrizzük, amíg a fiókod aktív, valamint egy ésszerű ideig a megszüntetés után is, hogy helyreálljunk a véletlen törlésekből, és teljesítsük a jogi és audit kötelezettségeket. A biztonsági mentéseket általában legfeljebb 90 napig őrizzük meg.

## Hol vagyunk

Ezt a szolgáltatást **{{operator.jurisdiction}}** területéről üzemeltetjük, és személyes adataid kezelésére ezen a szerveren {{operator.jurisdiction}} joga vonatkozik.

## Változások

Ezeket a feltételeket időről időre frissíthetjük. A lényegi változásokat azok hatálybalépése előtt bejelentjük. A szolgáltatás további használata egy módosítást követően azt jelenti, hogy elfogadod a frissített feltételeket.

## Kapcsolat

Bármely adatvédelmi kérdés vagy kérelem esetén: **{{operator.email}}**
