# A Mochi adatvédelme

Ez az adatvédelmi tájékoztató ismerteti, hogyan gyűjti, használja és osztja meg személyes adataidat a **{{operator.name}}** (e Mochi szerver üzemeltetője), amikor fiókot hozol létre vagy használod a szolgáltatást. A {{operator.name}} az adatkezelő.

## Mit gyűjtünk

A regisztrációkor és a szerver használata során a következőket gyűjtjük:

- **Fiókadatok**: felhasználónév, az általad választott hitelesítési módszerek (e-mail-cím, passkey-ek, hitelesítő alkalmazás, helyreállítási kódok vagy harmadik féltől származó belépési szolgáltató fiókja), valamint bármely megadott profilinformáció (megjelenítendő név, profilkép, életrajz).
- **Az általad létrehozott tartalom**: bejegyzések, üzenetek, hozzászólások, fájlok és minden egyéb adat, amelyet a szerveren telepített alkalmazásokon keresztül közzéteszel.
- **Technikai adatok**: IP-cím, böngésző- és eszközinformációk, valamint a kérések időbélyegei. Ezt a szolgáltatás biztonságának és működésének fenntartására, valamint visszaélések felderítésére használjuk.
- **Sütik**: bejelentkezéskor egy munkamenet-süti kerül beállításra. Ez csak egy átlátszatlan munkamenet-azonosítót tárol; nem használunk sütit reklámcélokra vagy keresztoldalas követésre.

Nem értékesítjük személyes adataidat, és nem osztjuk meg azokat hirdetőkkel.

## Hogyan kezeli a Mochi az adataidat (ez fontos)

A Mochi egy **peer-to-peer (P2P) hálózat**, nem egyetlen szerveres platform. Ez olyan módokon befolyásolja az adataid áramlását, amelyek számítanak a magánéleted szempontjából:

- A követőidnek, feliratkozóidnak vagy megosztott terekbe közzétett tartalom **más Mochi szerverekre is replikálódik**, amelyek ezeket a követőket vagy feliratkozókat tárolják. A {{operator.name}} nem tudja törölni a tartalmadról készült másolatokat olyan szerverekről, amelyeket nem ő üzemeltet. Ha törölsz egy bejegyzést, ez a szerver törlési értesítést küld azoknak a társgépeknek, amelyeknek korábban elküldte a bejegyzést, de a törlés legjobb erőfeszítésen alapul, és e társgépek együttműködésétől függ.
- Az **azonosságod (felhasználónév, profil, nyilvános kulcsok)** közzétételre kerül a Mochi névjegyzékben, ha nyilvános profilt választasz, így más Mochi felhasználók megtalálhatnak és kapcsolatba léphetnek veled.
- A közvetlen, privát tartalom (vázlatok, privát jegyzetek, meghatározott címzettek közötti privát üzenetek) csak ezen a szerveren és a címzettek szerverein marad; nem része a nyilvános P2P hálózatnak.

Röviden: bármi, amit úgy döntesz, hogy megosztasz másokkal a Mochin, tervezésénél fogva túljut a {{operator.name}} ellenőrzésén. Úgy kezeld a közzétett tartalmat, ahogyan az e-mailt vagy bármely más föderált kommunikációt kezelnéd.

## Harmadik felek

A kapcsolódó funkciók használatakor a szükséges minimális személyes adatot megosztjuk az alábbi harmadik felekkel:

- **E-mail-szolgáltatás**: a kimenő e-maileket (regisztrációs kódok, értesítések, jelszó-visszaállítások) a {{operator.name}} által beállított SMTP szolgáltatón keresztül küldjük.
- **Harmadik féltől származó belépési szolgáltatók**: ha Google, GitHub, Microsoft, Facebook vagy más OAuth szolgáltatóval lépsz be, az adott szolgáltató tudomást szerez arról, hogy beléptél erre a szerverre, és megosztja velünk az e-mail-címedet és az alapvető profiladatokat.
- **Fizetési szolgáltatók**: ha vásárlást végzel egy piactér-alkalmazáson keresztül, a fizetést a Stripe (vagy bármely, a {{operator.name}} által beállított fizetési szolgáltató) dolgozza fel. Nem tároljuk a teljes kártyaadatokat.

## A jogaid

Attól függően, hol élsz, jogod lehet hozzáférni személyes adataidhoz, helyesbíteni, exportálni, korlátozni vagy törölni azokat. E jogok bármelyikének gyakorlásához – beleértve a fiókod megszüntetését és az adataid eltávolítását erről a szerverről – írj a **{{operator.email}}** címre. A fiók megszüntetését jelenleg a szerverüzemeltető végzi kérésre; ha megérkezik az e-mailed, lezárjuk a fiókot, és eltávolítjuk a kapcsolódó adatokat erről a szerverről (a biztonsági mentések megőrzésére és bármely meghatározott nyilvántartás megőrzésére vonatkozó jogi kötelezettségre figyelemmel).

## Adatmegőrzés

A fiókadatokat addig őrizzük, amíg a fiókod aktív, valamint egy ésszerű ideig a megszüntetés után is, hogy helyreálljunk a véletlen törlésekből, és teljesítsük a jogi és audit kötelezettségeket. A biztonsági mentéseket általában legfeljebb 90 napig őrizzük meg.

## Hol vagyunk

Ezt a szolgáltatást **{{operator.jurisdiction}}** területéről üzemeltetjük, és személyes adataid kezelésére ezen a szerveren {{operator.jurisdiction}} joga vonatkozik.

## Változások

Ezeket a feltételeket időről időre frissíthetjük. A lényegi változásokat azok hatálybalépése előtt bejelentjük. A szolgáltatás további használata egy módosítást követően azt jelenti, hogy elfogadod a frissített feltételeket.

## Kapcsolat

Bármely adatvédelmi kérdés vagy kérelem esetén: **{{operator.email}}**
