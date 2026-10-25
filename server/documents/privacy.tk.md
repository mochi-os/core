# Mochi Gizlinlik

Bu gizlinlik bildirişi **{{operator.name}}** (bu Mochi serweriniň operatory) hasap döredeniňizde ýa-da hyzmatdan peýdalananyňyzda şahsy maglumatlaryňyzy nähili ýygnaýandygyny, ulanýandygyny we paýlaşýandygyny düşündirýär. {{operator.name}} maglumatlary dolandyryjydyr.

## Näme ýygnaýarys

Bu serwere agza bolup hyzmatdan peýdalananyňyzda şulary ýygnaýarys:

- **Hasap maglumatlary**: ulanyjy ady, saýlap alýan tassyklaýyş usullaryňyz (e-poçta salgysy, passkey, autentifikator programma, dikeldiş kodlary ýa-da üçünji tarap giriş üpjünçisindäki hasap), we hasabyňyza goşýan profil maglumatlaryňyz (görkezilýän at, awatar, terjimehal).
- **Döredýän mazmunyňyz**: bu serwerde gurnalan programmalar arkaly çap edýän ýazgylaryňyz, habarlaryňyz, teswirleriňiz, faýllaryňyz we başga islendik maglumatlar.
- **Başgalardan gelýän mazmun**: beýleki Mochi serwerlerindäki we beýleki torlardaky adamlaryň ýazgylary, habarlary we profil maglumatlary; olar bu serwere siziň ýa-da beýleki ulanyjylaryň olary yzarlaýandygy, olara abonent bolýandygy ýa-da olar bilen hat alyşýandygy sebäpli gelip ýetýär.
- **Şikaýatlar**: Mochi-de ýa-da başga bir tordan, siziň kimdir biri barada eden şikaýatyňyz ýa-da kimdir biriniň siz barada eden şikaýaty bu serweriň administratorlarynyň çäre görmegi üçin saklanýar.
- **Tehniki maglumatlar**: IP salgysy, brauzer/enjam maglumatlary we soraglaryňyzyň wagt belgileri. Bular hyzmaty howpsuz we işjeň saklamak hem-de hyýanatçylygy ýüze çykarmak üçin ulanylýar.
- **Cookie**: girizilende sessiýa cookie bellenýär. Onda diňe açylmaýan sessiýa belgisi saklanýar; biz cookie-leri mahabatlandyrmak ýa-da saýtara yzarlaýyş üçin ulanmaýarys.

Şahsy maglumatlaryňyzy satmaýarys we mahabatçylar bilen paýlaşmaýarys.

## Mochi maglumatlaryňyzy nähili ulanýar (bu möhümdir)

Mochi **deňme-deň (peer-to-peer, P2P) tory**, ýekeje serwerli platforma däl. Bu maglumatlaryňyzyň gizlinligi üçin ähmiýetli bolan akymyny üýtgedýär:

- Yzarlaýjylara, abonentlere ýa-da paýlaşylan giňişliklere çap edýän mazmunyňyz şol yzarlaýjylary ýa-da abonentleri ýerleşdirýän **beýleki Mochi serwerlerine göçürilýär**. {{operator.name}} öz işletmeýän serwerlerinden mazmunyňyzyň göçürmelerini aýryp bilmeýär. Eger ýazgyny pozsaňyz, bu serwer ozal ýazgyny iberen deňmeleriniň hemmesine pozuş habarnamasyny iberer, emma poziş iň gowy synanyşyk hökmünde işleýär we şol deňmeleriň hyzmatdaşlygyna baglydyr.
- Açyk profili saýlasaňyz, **şahsyýetiňiz (ulanyjy ady, profil, açyk açarlar)** Mochi katalogyna çap edilýär, şonuň üçin beýleki Mochi ulanyjylary sizi tapyp we siziň bilen habarlaşyp bilerler.
- Göni, ýapyk mazmun (taslamalar, hususy bellikler, belli alyjylaryň arasyndaky hususy habarlar) diňe bu serwerde we alyjylaryň serwerlerinde galýar; ol köpçülikleýin P2P toruň bölegi däldir.
- Bu serwer size **beýleki torlara birikmäge** mümkinçilik berýän ýerinde – başga bir hyzmatda bar bolan hasabyňyzy baglanyşdyrmak bilen ýa-da beýleki torlardaky adamlaryň yzarlap ýa-da oňa habar iberip biljek hasabyny şu ýerde ýerleşdirmek bilen – şol hasap arkaly çap edýän ýa-da iberýän zatlaryňyz şol torlardaky serwerlere gidýär. Olar Mochi serwerleri däl we {{operator.name}} olary işletmeýär. Şeýle hasap öz torunda açykdyr, käbir torlarda bolsa yzarlaýan hasaplaryňyz, halaýandygyňyzy bellän ýazgylaryňyz we Bluesky-de bloklaýan adamlaryňyz hem açykdyr. Soňra bir zady redaktirlemek ýa-da pozmak şol serwerleriň äsgermezlik edip biljek haýyşyny iberýär.
- Mochi-de we beýleki torlarda söhbetdeşlik edýän adamlaryňyz haçan onlaýndygyňyzy görüp bilýärler. Muny söhbet programmasynyň sazlamalarynda **Onlaýndygymy görkez** arkaly öçürip bilersiňiz.
- Başga bir serwerdäki kimdir biri barada eden şikaýatyňyz şol serweriň moderatorlaryna hem iberilip bilner, siziň däl-de, bu serweriň şikaýaty hökmünde: onda ony kimiň edendigi aýdylmaýar.

Gysgaça: Mochi-de ýa-da beýleki torlarda başgalar bilen paýlaşmagy saýlaýan zatlaryňyz tasaryň özi boýunça {{operator.name}}-iň gözegçiliginden çykyp gider. Çap edýän mazmunyňyza e-poçta ýa-da islendik federatiw aragatnaşyk bilen deň çemeleşiň.

## Üçünji taraplar

Degişli mümkinçilikleri ulananyňyzda şu üçünji taraplar bilen iň az şahsy maglumat paýlaşýarys:

- **E-poçta hyzmaty**: çykyş e-poçtalary (giriş kodlary, salgy tassyklamasy, habarnamalar) {{operator.name}} tarapyndan düzülen SMTP üpjünçisi arkaly iberilýär.
- **Üçünji tarap giriş üpjünçileri**: Google, GitHub, Microsoft, Facebook ýa-da başga OAuth üpjünçisi bilen girseňiz, şol üpjünçi bu serwere gireniňizi bilýär we e-poçta salgyňyzy hem-de esasy profil maglumatlaryňyzy biziň bilen paýlaşýar.
- **Töleg üpjünçileri**: market programmasy arkaly satyn alyş etseňiz, töleg Stripe (ýa-da {{operator.name}} tarapyndan düzülen başga töleg üpjünçisi) tarapyndan amal edilýär. Doly kart maglumatlaryny saklamaýarys.
- **Push habarnama hyzmatlary**: Android-däki Mochi programmasyna habarnamalar Google-yň Firebase Cloud Messaging hyzmaty arkaly gowşurylýar, ol olaryň sözbaşysyny we tekstini geçirýär. Bu serwere başga bir programma, meselem, Mastodon, Matrix ýa-da XMPP programmasy bilen girseňiz, onuň habarnamalary şol programmanyň ulanýan push hyzmaty arkaly gowşurylýar we habarnamanyň degişli bolan habaryny öz içine alyp biler.
- **Sertifikat edarasy**: bu serwer Bluesky hasaplaryny öz domeniniň aşagynda ýerleşdirýän ýerinde, her hasabyň salgysy üçin howpsuzlyk sertifikaty berilýär. Sertifikat edaralary beren her bir sertifikatyny köpçülikleýin žurnallarda çap edýärler, şonuň üçin şol salgylar şol ýerde sanawda görkezilýär.
- **Beýleki torlar**: başga bir hyzmatda bar bolan hasabyňyzy baglanyşdyrsaňyz, şol hyzmat siziň ol arkaly çap edýän zatlaryňyzy alýar, bu serwer bolsa özüne şol ýerde siziň adyňyzdan hereket etmäge mümkinçilik berýän tassyklaýyş maglumatyny saklaýar. Bu ýerde beýleki tor üçin hasap ýerleşdirseňiz, şol toruň serwerleri we hyzmatlary siziň oňa çap edýän zatlaryňyzy hasabyň açyk salgysy bilen bilelikde alýar.

## Hukuklaryňyz

Ýaşaýan ýeriňize görä, şahsy maglumatlaryňyza girmek, düzeltmek, eksport etmek, çäklendirmek ýa-da pozmak hukugyňyz bolup biler. Maglumatlaryňyzy eksport etmegi we hasabyňyzy ýapmagy Sazlamalarda özüňiz edip bilersiňiz. Hasabyňyzy ýapmak ony ýeňillik möhletine gulplaýar, şol möhletde girip, ýapmagy ýatyryp bilersiňiz; ondan soň hasap we onuň maglumatlary bu serwerden aýrylýar (ätiýaçlyk göçürme saklamasyna we belli ýazgylary saklamak hakda ýuridiki borçlara baglylykda). Başga islendik hukugy amala aşyrmak üçin ýa-da girip bilmeýän bolsaňyz, **{{operator.email}}** salgysyna ýazyň.

## Maglumat saklama

Hasap maglumatlaryňyzy hasabyňyz işjeň bolan döwründe, hem-de ýapylandan soň tötänleýin pozulmadan dikeltmek we ýuridiki we audit borçlary ýerine ýetirmek üçin makul wagt aralygynda saklaýarys. Ätiýaçlyk göçürmeler köplenç 90 güne çenli saklanýar.

## Niredediris

Bu hyzmat **{{operator.jurisdiction}}** ýurisdiksiýasyndan dolandyrylýar we bu serwerdäki şahsy maglumatlaryňyzyň işlenmegine {{operator.jurisdiction}} kanuny ulanylýar.

## Üýtgeşmeler

Bu şertleri wagtal-wagtal täzeläp bileris. Möhüm üýtgeşmeler güýje girmezinden öň yglan ediler. Üýtgeşmeden soň hyzmaty dowam etdirip ulanmagyňyz täzelenen şertleri kabul edýändigiňizi aňladýar.

## Habarlaşma

Islendik gizlinlik soragy ýa-da haýyş üçin: **{{operator.email}}**
