# Mochi privaatsus

Käesolev privaatsusteatis selgitab, kuidas **{{operator.name}}** (selle Mochi serveri operaator) kogub, kasutab ja jagab teie isikuandmeid, kui loote konto või kasutate teenust. {{operator.name}} on andmete vastutav töötleja.

## Mida me kogume

Kui registreerute ja kasutate seda serverit, kogume:

- **Konto andmed**: kasutajanimi, teie valitud autentimismeetodid (e-posti aadress, pääsuvõtmed, autentimisrakendus, taastekoodid või kolmanda osapoole sisselogimisteenuse pakkuja konto) ja mis tahes profiiliteave, mille lisate (kuvatav nimi, profiilipilt, biograafia).
- **Teie loodud sisu**: postitused, sõnumid, kommentaarid, failid ja muud andmed, mille avaldamise valite läbi sellesse serverisse paigaldatud rakenduste.
- **Teiste inimeste sisu**: teistes Mochi serverites ja teistes võrkudes olevate inimeste postitused, sõnumid ja profiiliandmed, mis jõuavad sellesse serverisse, kuna teie või teised kasutajad neid jälgivad, tellivad või nendega kirjavahetust peavad.
- **Teated**: teade, mille te kellegi kohta esitate või mille keegi teie kohta esitab, Mochis või mõnest muust võrgust, säilitatakse, et selle serveri administraatorid saaksid selle põhjal tegutseda.
- **Tehnilised andmed**: IP-aadress, brauseri/seadme teave ja teie tehtud päringute ajatemplid. Neid kasutatakse teenuse turvalisuse ja toimimise tagamiseks ning väärkasutuse tuvastamiseks.
- **Küpsised**: sisselogimisel seatakse seansiküpsis. See salvestab ainult läbipaistmatu seansi-identifikaatori; me ei kasuta küpsiseid reklaami ega saidiülese jälgimise eesmärgil.

Me ei müü teie isikuandmeid ega jaga neid reklaamijatega.

## Kuidas Mochi käitleb teie andmeid (see on oluline)

Mochi on **partnervõrgu (P2P) võrgustik**, mitte üksiku serveri platvorm. See muudab teie andmete liikumist viisil, mis on teie privaatsuse seisukohalt oluline:

- Sisu, mille avaldate jälgijatele, tellijatele või jagatud aladele, **paljundatakse teistesse Mochi serveritesse**, mis majutavad neid jälgijaid või tellijaid. {{operator.name}} ei saa kustutada teie sisu koopiaid serveritest, mida ta ei halda. Kui kustutate postituse, saadab see server kustutamisteate partneritele, kellele postitus on varem saadetud, kuid kustutamine on parima püüdlusega ja sõltub nende partnerite koostöövalmidusest.
- Teie **identiteet (kasutajanimi, profiil, avalikud võtmed)** avaldatakse Mochi kataloogis, kui valite avaliku profiili, et teised Mochi kasutajad saaksid teid leida ja teiega ühendust võtta.
- Otsene, eraviisiline sisu (mustandid, eraviisilised märkmed, eraviisilised sõnumid konkreetsete saajate vahel) jääb ainult sellesse serverisse ja saajate serveritesse; see ei ole osa avalikust P2P-võrgust.
- Kui see server võimaldab teil **ühenduda teiste võrkudega** - sidudes konto, mis teil on mõnes muus teenuses, või majutades siin kontot, mida teiste võrkude inimesed saavad jälgida või millele nad saavad sõnumeid saata - liigub see, mida te selle konto kaudu avaldate või saadate, nende võrkude serveritesse. Need ei ole Mochi serverid ja {{operator.name}} ei halda neid. Selline konto on oma võrgus avalik ning mõnes võrgus on avalikud ka kontod, mida te jälgite, postitused, mis teile meeldivad, ja Bluesky puhul inimesed, keda te blokeerite. Millegi hilisem muutmine või kustutamine saadab päringu, mida need serverid võivad eirata.
- Inimesed, kellega te vestlete, Mochis ja teistes võrkudes, näevad, millal te olete võrgus. Saate selle välja lülitada vestlusrakenduse seadetes valikuga **Näita, kui olen võrgus**.
- Teade, mille te esitate kellegi kohta teises serveris, võidakse saata ka tolle serveri moderaatoritele, käesoleva serveri teatena, mitte teie omana: selles ei öelda, kes selle esitas.

Lühidalt: kõik, mida valite teiste inimestega jagada, Mochis või teistes võrkudes, liigub kavakohaselt {{operator.name}} kontrolli alt välja. Suhtuge sisu, mille avaldate, nagu suhtuksite e-kirja või muusse föderatiivsesse kommunikatsiooni.

## Kolmandad osapooled

Me jagame minimaalselt vajalikke isikuandmeid järgmiste kolmandate osapooltega, kui kasutate seotud funktsioone:

- **E-posti teenus**: väljaminevad e-kirjad (sisselogimiskoodid, aadressi kinnitamine, teavitused) saadetakse {{operator.name}} seadistatud SMTP-teenusepakkuja kaudu.
- **Kolmanda osapoole sisselogimisteenuse pakkujad**: kui logite sisse Google'i, GitHubi, Microsofti, Facebooki või mõne muu OAuth-pakkuja kaudu, saab see pakkuja teada, et logisite sellesse serverisse sisse, ning jagab meiega teie e-posti aadressi ja põhilist profiiliteavet.
- **Maksete teenusepakkujad**: kui teete ostu turuplatsi rakenduse kaudu, töötleb makset Stripe (või mis tahes muu makseteenuse pakkuja, mille {{operator.name}} on seadistanud). Me ei säilita kaardi täielikke andmeid.
- **Tõuketeavituste teenused**: teavitused Mochi rakendusele Androidis edastatakse Google'i Firebase Cloud Messaging teenuse kaudu, mis kannab nende pealkirja ja teksti. Kui logite sellesse serverisse sisse mõne muu rakendusega, näiteks Mastodoni, Matrixi või XMPP rakendusega, edastatakse selle teavitused tõuketeenuse kaudu, mida see rakendus kasutab, ning need võivad sisaldada sõnumit, mida teavitus puudutab.
- **Sertifitseerimiskeskus**: kui see server majutab Bluesky kontosid oma domeeni all, väljastatakse iga konto aadressile turvasertifikaat. Sertifitseerimiskeskused avaldavad iga väljastatud sertifikaadi avalikes logides, seega on need aadressid seal loetletud.
- **Teised võrgud**: kui seote konto, mis teil on mõnes muus teenuses, saab see teenus selle, mida te selle kaudu postitate, ja see server salvestab mandaadi, mis võimaldab tal seal teie nimel tegutseda. Kui majutate siin kontot mõne muu võrgu jaoks, saavad selle võrgu serverid ja teenused selle, mida te sellesse võrku avaldate, koos konto avaliku aadressiga.

## Teie õigused

Sõltuvalt teie elukohast võivad teil olla õigused oma isikuandmetega tutvuda, neid parandada, eksportida, piirata või kustutada. Saate oma andmed eksportida ja konto ise sulgeda Seadetes. Konto sulgemine lukustab selle armuajaks, mille jooksul saate sisse logida ja sulgemise tühistada; pärast seda eemaldatakse konto ja selle andmed sellest serverist (kui varukoopiate säilitamine ja seaduslik kohustus konkreetseid andmeid säilitada seda ei piira). Muu õiguse teostamiseks või kui te ei saa sisse logida, saatke e-kiri aadressile **{{operator.email}}**.

## Andmete säilitamine

Me säilitame teie konto andmeid niikaua, kui teie konto on aktiivne, ja mõistliku aja pärast sulgemist juhusliku kustutamise korral taastamiseks ning seaduslike ja audiitorlike kohustuste täitmiseks. Varukoopiaid säilitatakse tavaliselt kuni 90 päeva.

## Kus me asume

Seda teenust opereeritakse riigist **{{operator.jurisdiction}}** ja teie isikuandmete töötlemisele selles serveris kohaldub {{operator.jurisdiction}} õigus.

## Muudatused

Me võime neid tingimusi aeg-ajalt uuendada. Olulistest muudatustest teatatakse enne nende jõustumist. Teenuse jätkuv kasutamine pärast muudatust tähendab, et nõustute uuendatud tingimustega.

## Kontakt

Privaatsust puudutavate küsimuste korral või taotluse esitamiseks: **{{operator.email}}**
