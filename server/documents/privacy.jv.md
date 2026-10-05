# Privasi Mochi

Sesorah privasi punika nerangaken kadospundi **{{operator.name}}** (operator server Mochi punika) ngempalaken, ngginakaken, lan mbagi data pribadi panjenengan nalika panjenengan damel akun utawi ngginakaken layanan. {{operator.name}} inggih punika pangendali data.

## Punapa ingkang kawula kempalaken

Nalika panjenengan ndaftar lan ngginakaken server punika, kawula ngempalaken:

- **Data akun**: nami pangguna, cara autentikasi ingkang panjenengan pilih (alamat email, passkey, aplikasi authenticator, kode pamulihan, utawi akun saking panyadhiya login pihak katelu), saha katrangan profil punapa kemawon ingkang panjenengan tambahaken (nami tampilan, avatar, biografi).
- **Konten ingkang panjenengan damel**: postingan, pesen, komentar, file, lan data sanesipun ingkang panjenengan pilih kangge dipun terbitaken lumantar aplikasi ingkang dipasang ing server punika.
- **Konten saking tiyang sanes**: postingan, pesen, saha rinci profil tiyang-tiyang ing server Mochi sanes lan ing jaringan sanes, ingkang dumugi server punika amargi panjenengan utawi pangguna sanes ngikuti, langganan, utawi korespondensi kaliyan tiyang-tiyang kasebut.
- **Data teknis**: alamat IP, katrangan browser/piranti, lan cap wekdal kangge panyuwunan ingkang panjenengan damel. Punika dipun ginakaken kangge njagi keamanan saha operasi layanan, sarta kangge ndeteksi panyalahgunaan.
- **Cookie**: cookie sesi dipun setel nalika panjenengan mlebet. Cookie punika namung nyimpen pengenal sesi ingkang buram; kawula boten ngginakaken cookie kangge iklan utawi pelacakan lintas situs.

Kawula boten nyade data pribadi panjenengan, lan kawula boten mbagi dhateng pangiklan.

## Kadospundi Mochi nangani data panjenengan (punika wigatos)

Mochi inggih punika **jaringan peer-to-peer (P2P)**, sanes platform server tunggal. Bab punika ngewahi alur data panjenengan kanthi cara ingkang wigati tumrap privasi panjenengan:

- Konten ingkang panjenengan terbitaken dhateng pengikut, pelanggan, utawi papan ingkang dipun bagi **dipun replikasi dhateng server Mochi sanes** ingkang nampung pengikut utawi pelanggan kasebut. {{operator.name}} boten saged mbusek salinan konten panjenengan saking server ingkang boten dipun jalanaken piyambakipun. Menawi panjenengan mbusek postingan, server punika badhe ngintunaken sesorah panghapusan dhateng peer ingkang sampun nampi postingan kasebut, ananging panghapusan punika sipatipun usaha sak sagedipun lan gumantung dhateng kerja sama saking peer-peer kasebut.
- **Identitas panjenengan (nami pangguna, profil, kunci umum)** dipun terbitaken dhateng direktori Mochi menawi panjenengan milih profil umum, supados pangguna Mochi sanes saged manggihaken lan ngubungi panjenengan.
- Konten pribadi langsung (draf, cathetan pribadi, pesen pribadi antawisipun panampi tartamtu) namung tetep wonten ing server punika lan server panampinipun; punika sanes bagean saking jaringan P2P umum.
- Menawi server punika ngidinaken panjenengan **nyambung dhateng jaringan sanes** - kanthi nyambungaken akun ingkang panjenengan gadhahi ing layanan sanes, utawi kanthi nampung akun ing ngriki ingkang saged dipun ikuti utawi dipun kintuni pesen dening tiyang-tiyang ing jaringan sanes - punapa ingkang panjenengan terbitaken utawi kintunaken lumantar akun kasebut badhe dumugi server-server ing jaringan kasebut. Server-server punika sanes server Mochi lan boten dipun jalanaken dening {{operator.name}}. Akun ingkang kados makaten punika umum ing jaringanipun, lan ing sawetawis jaringan, akun ingkang panjenengan ikuti saha postingan ingkang panjenengan remeni ugi umum. Ngowahi utawi mbusek satunggaling bab ing tembe badhe ngintunaken panyuwunan ingkang saged dipun lirwakaken dening server-server kasebut.

Cekakipun: punapa kemawon ingkang panjenengan pilih kangge dipun bagi dhateng tiyang sanes, ing Mochi utawi ing jaringan sanes, badhe nglangkungi kendali {{operator.name}}, miturut rancangan. Tanganana konten ingkang panjenengan terbitaken kados dene email utawi komunikasi terfederasi sanesipun.

## Pihak katelu

Kawula mbagi data pribadi minimum ingkang dipun perlokaken dhateng pihak katelu ing ngandhap punika nalika panjenengan ngginakaken fitur ingkang gegandhengan:

- **Layanan email**: email metu (kode mlebet, verifikasi alamat, notifikasi) dipun kintunaken lumantar panyadhiya SMTP ingkang dipun konfigurasi dening {{operator.name}}.
- **Panyadhiya login pihak katelu**: menawi panjenengan mlebet kanthi Google, GitHub, Microsoft, Facebook, utawi panyadhiya OAuth sanesipun, panyadhiya kasebut mangertos bilih panjenengan sampun mlebet ing server punika lan mbagi alamat email saha data profil dhasar panjenengan dhateng kawula.
- **Panyadhiya pambayaran**: menawi panjenengan tumbas lumantar aplikasi marketplace, pambayaran dipun proses dening Stripe (utawi panyadhiya pambayaran punapa kemawon ingkang sampun dipun konfigurasi {{operator.name}}). Kawula boten nyimpen rinci kartu pepak.
- **Jaringan sanes**: menawi panjenengan nyambungaken akun ingkang panjenengan gadhahi ing layanan sanes, layanan kasebut nampi punapa ingkang panjenengan postingaken lumantar akun kasebut, lan server punika nyimpen kredensial ingkang ngidinaken server punika tumindak makili panjenengan wonten ing ngriku. Menawi panjenengan nampung akun ing ngriki kangge jaringan sanes, server saha layanan jaringan kasebut nampi punapa ingkang panjenengan terbitaken dhateng jaringan kasebut, sesarengan kaliyan alamat umum akun kasebut.

## Hak panjenengan

Gumantung saking papan panggenan panjenengan, panjenengan saged gadhah hak kangge ngakses, ndandosi, ngekspor, mbatesi, utawi mbusek data pribadi panjenengan. Panjenengan saged ngekspor data panjenengan lan nutup akun panjenengan piyambak ing Setelan. Nutup akun badhe ngunci akun kasebut salami wekdal tenggang, ing salebetipun wekdal punika panjenengan saged mlebet lan mbatalaken; sasampunipun punika akun saha datanipun dipun icalaken saking server punika (tundhuk dhateng retensi cadhangan saha kewajiban hukum punapa kemawon kangge nyimpen cathetan tartamtu). Kangge ngginakaken hak sanesipun, utawi menawi panjenengan boten saged mlebet, kintunaken email dhateng **{{operator.email}}**.

## Retensi data

Kawula nyimpen data akun panjenengan salaminipun akun panjenengan aktif, saha kangge wekdal ingkang wajar sasampunipun panutupan kangge mulihaken saking panghapusan ingkang boten dipun sengaja sarta kangge nyekapi kewajiban hukum lan audit. Cadhangan limrahipun dipun simpen ngantos 90 dinten.

## Papan kawula

Layanan punika dipun operasekaken saking **{{operator.jurisdiction}}**, lan hukum {{operator.jurisdiction}} ditrapaken kangge pamrosesan data pribadi panjenengan ing server punika.

## Pangewahan

Kawula saged ngenggalaken pranatan punika saking wekdal dhateng wekdal. Pangewahan material badhe dipun umumaken saderengipun mlampah. Panganggening layanan ingkang lajeng sasampunipun pangewahan ateges panjenengan nampi pranatan ingkang sampun dipun enggalaken.

## Kontak

Kangge pitakenan privasi punapa kemawon utawi ndamel panyuwunan: **{{operator.email}}**
