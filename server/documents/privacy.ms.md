# Privasi Mochi

Notis privasi ini menerangkan cara **{{operator.name}}** (pengendali pelayan Mochi ini) mengumpul, menggunakan, dan berkongsi data peribadi anda apabila anda mencipta akaun atau menggunakan perkhidmatan ini. {{operator.name}} ialah pengawal data.

## Apa yang kami kumpul

Apabila anda mendaftar dan menggunakan pelayan ini, kami mengumpul:

- **Data akaun**: nama pengguna, kaedah pengesahan yang anda pilih (alamat e-mel, passkey, aplikasi pengesah, kod pemulihan, atau akaun daripada pembekal log masuk pihak ketiga), dan apa-apa maklumat profil yang anda tambah (nama paparan, avatar, biografi).
- **Kandungan yang anda cipta**: siaran, mesej, komen, fail, dan apa-apa data lain yang anda pilih untuk diterbitkan melalui aplikasi yang dipasang pada pelayan ini.
- **Kandungan daripada orang lain**: siaran, mesej, dan butiran profil orang di pelayan Mochi lain dan di rangkaian lain, yang sampai ke pelayan ini kerana anda atau pengguna lain mengikuti, melanggan, atau berutus surat dengan mereka.
- **Laporan**: laporan yang anda buat tentang seseorang, atau yang dibuat oleh seseorang tentang anda, di Mochi atau daripada rangkaian lain, disimpan supaya pentadbir pelayan ini boleh mengambil tindakan terhadapnya.
- **Data teknikal**: alamat IP, maklumat pelayar/peranti, dan cap masa untuk permintaan yang anda buat. Ini digunakan untuk memastikan perkhidmatan selamat dan beroperasi, dan untuk mengesan penyalahgunaan.
- **Kuki**: kuki sesi ditetapkan apabila anda log masuk. Ia hanya menyimpan pengecam sesi yang legap; kami tidak menggunakan kuki untuk pengiklanan atau penjejakan merentas tapak.

Kami tidak menjual data peribadi anda, dan kami tidak berkongsinya dengan pengiklan.

## Bagaimana Mochi mengendalikan data anda (ini penting)

Mochi ialah **rangkaian peer-to-peer (P2P)**, bukan platform pelayan tunggal. Itu mengubah cara data anda mengalir dengan cara yang penting untuk privasi anda:

- Kandungan yang anda terbitkan kepada pengikut, pelanggan, atau ruang dikongsi **direplikasi ke pelayan Mochi lain** yang menjadi tuan rumah pengikut atau pelanggan tersebut. {{operator.name}} tidak boleh memadamkan salinan kandungan anda daripada pelayan yang ia tidak kendalikan. Jika anda memadamkan siaran, pelayan ini akan menghantar notis pemadaman kepada peer yang ia telah hantar siaran itu sebelum ini, tetapi pemadaman itu adalah usaha terbaik dan bergantung pada kerjasama peer tersebut.
- **Identiti anda (nama pengguna, profil, kunci awam)** diterbitkan ke direktori Mochi jika anda memilih profil awam, supaya pengguna Mochi lain boleh mencari dan menghubungi anda.
- Kandungan persendirian dan langsung (draf, nota peribadi, mesej peribadi antara penerima tertentu) kekal di pelayan ini dan pelayan penerima sahaja; ia bukan sebahagian daripada rangkaian P2P awam.
- Jika pelayan ini membenarkan anda **menyambung ke rangkaian lain** - dengan memautkan akaun yang anda pegang di perkhidmatan lain, atau dengan menjadi tuan rumah akaun di sini yang boleh diikuti atau dihantar mesej oleh orang di rangkaian lain - apa yang anda terbitkan atau hantar melalui akaun itu pergi ke pelayan di rangkaian tersebut. Pelayan tersebut bukan pelayan Mochi dan {{operator.name}} tidak mengendalikannya. Akaun seperti ini adalah awam di rangkaiannya, dan di sesetengah rangkaian, akaun yang anda ikuti, siaran yang anda sukai dan, di Bluesky, orang yang anda sekat juga awam. Mengedit atau memadamkan sesuatu kemudian akan menghantar permintaan yang mungkin diabaikan oleh pelayan tersebut.
- Orang yang bersembang dengan anda, di Mochi dan di rangkaian lain, boleh melihat bila anda dalam talian. Anda boleh mematikan ini dengan **Tunjukkan apabila saya dalam talian** dalam tetapan aplikasi sembang.
- Laporan yang anda buat tentang seseorang di pelayan lain juga mungkin dihantar kepada moderator pelayan tersebut, sebagai laporan pelayan ini dan bukan laporan anda: laporan itu tidak menyatakan siapa yang membuatnya.

Pendek kata: apa-apa yang anda pilih untuk dikongsi dengan orang lain, di Mochi atau di rangkaian lain, akan bergerak di luar kawalan {{operator.name}}, secara reka bentuk. Anggap kandungan yang anda terbitkan seperti anda menganggap e-mel atau apa-apa komunikasi bersekutu lain.

## Pihak ketiga

Kami berkongsi data peribadi minimum yang diperlukan dengan pihak ketiga berikut apabila anda menggunakan ciri yang berkaitan:

- **Perkhidmatan e-mel**: e-mel keluar (kod log masuk, pengesahan alamat, pemberitahuan) dihantar melalui pembekal SMTP yang dikonfigurasi oleh {{operator.name}}.
- **Pembekal log masuk pihak ketiga**: jika anda log masuk dengan Google, GitHub, Microsoft, Facebook, atau pembekal OAuth lain, pembekal tersebut mengetahui bahawa anda log masuk ke pelayan ini dan berkongsi alamat e-mel dan data profil asas anda dengan kami.
- **Pembekal pembayaran**: jika anda membuat pembelian melalui aplikasi pasaran, pembayaran diproses oleh Stripe (atau mana-mana pembekal pembayaran yang telah dikonfigurasi oleh {{operator.name}}). Kami tidak menyimpan butiran kad penuh.
- **Perkhidmatan pemberitahuan tolak**: pemberitahuan kepada aplikasi Mochi pada Android dihantar melalui Firebase Cloud Messaging milik Google, yang membawa tajuk dan teks pemberitahuan tersebut. Jika anda log masuk ke pelayan ini dengan aplikasi lain, seperti aplikasi Mastodon, Matrix atau XMPP, pemberitahuannya dihantar melalui perkhidmatan tolak yang digunakan oleh aplikasi itu, dan mungkin mengandungi mesej yang menjadi perkara pemberitahuan tersebut.
- **Pihak berkuasa sijil**: jika pelayan ini menjadi tuan rumah akaun Bluesky di bawah domainnya sendiri, alamat setiap akaun dikeluarkan sijil keselamatan. Pihak berkuasa sijil menerbitkan setiap sijil yang mereka keluarkan dalam log awam, jadi alamat tersebut disenaraikan di sana.
- **Rangkaian lain**: jika anda memautkan akaun yang anda pegang di perkhidmatan lain, perkhidmatan itu menerima apa yang anda siarkan melaluinya, dan pelayan ini menyimpan kelayakan yang membolehkannya bertindak bagi pihak anda di sana. Jika anda menjadi tuan rumah akaun di sini untuk rangkaian lain, pelayan dan perkhidmatan rangkaian itu menerima apa yang anda terbitkan kepadanya, bersama alamat awam akaun tersebut.

## Hak anda

Bergantung kepada tempat anda tinggal, anda mungkin mempunyai hak untuk mengakses, membetulkan, mengeksport, menyekat, atau memadam data peribadi anda. Anda boleh mengeksport data anda dan menutup akaun anda sendiri dalam Tetapan. Menutup akaun anda akan menguncinya untuk satu tempoh tangguh, dan dalam tempoh itu anda boleh log masuk dan membatalkan penutupan; selepas itu akaun dan datanya dialih keluar dari pelayan ini (tertakluk kepada pengekalan sandaran dan apa-apa kewajipan undang-undang untuk mengekalkan rekod tertentu). Untuk menggunakan mana-mana hak lain, atau jika anda tidak dapat log masuk, e-melkan **{{operator.email}}**.

## Pengekalan data

Kami menyimpan data akaun anda selagi akaun anda aktif, dan untuk tempoh munasabah selepas penutupan untuk pulih daripada pemadaman tidak sengaja dan untuk memenuhi kewajipan undang-undang dan audit. Sandaran biasanya dikekalkan sehingga 90 hari.

## Di mana kami berada

Perkhidmatan ini dikendalikan dari **{{operator.jurisdiction}}**, dan undang-undang {{operator.jurisdiction}} terpakai kepada pemprosesan data peribadi anda di pelayan ini.

## Perubahan

Kami mungkin mengemas kini terma ini dari semasa ke semasa. Perubahan material akan diumumkan sebelum ia berkuat kuasa. Penggunaan perkhidmatan secara berterusan selepas perubahan bermaksud anda menerima terma yang dikemas kini.

## Hubungi

Untuk apa-apa soalan privasi atau untuk membuat permintaan: **{{operator.email}}**
