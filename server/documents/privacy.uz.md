# Mochi maxfiyligi

Ushbu maxfiylik bildirishnomasi **{{operator.name}}**ning (ushbu Mochi serverining operatori) hisob ochganingizda yoki xizmatdan foydalanganingizda shaxsiy maʼlumotlaringizni qanday yigʻishi, ulardan qanday foydalanishi va ularni qanday ulashishini tushuntiradi. {{operator.name}} maʼlumotlar nazoratchisi hisoblanadi.

## Biz nimalarni yigʻamiz

Ushbu serverda roʻyxatdan oʻtganingizda va undan foydalanganingizda biz quyidagilarni yigʻamiz:

- **Hisob maʼlumotlari**: foydalanuvchi nomi, siz tanlagan autentifikatsiya usullari (elektron pochta manzili, passkeylar, autentifikator ilovasi, tiklash kodlari yoki uchinchi tomon kirish provayderidagi hisob) va siz qoʻshgan profil maʼlumotlari (koʻrinadigan ism, avatar, biografiya).
- **Siz yaratgan kontent**: ushbu serverga oʻrnatilgan ilovalar orqali eʼlon qilishni tanlagan postlar, xabarlar, izohlar, fayllar va boshqa har qanday maʼlumotlar.
- **Boshqalardan kelgan kontent**: boshqa Mochi serverlari va boshqa tarmoqlardagi odamlarning postlari, xabarlari va profil maʼlumotlari; ular ushbu serverga siz yoki boshqa foydalanuvchilar ularni kuzatgani, ularga obuna boʻlgani yoki ular bilan yozishgani sababli yetib keladi.
- **Texnik maʼlumotlar**: siz yuborgan soʻrovlar uchun IP-manzil, brauzer/qurilma maʼlumotlari va vaqt belgilari. Bular xizmatni xavfsiz hamda ishchi holatda saqlash va suiisteʼmolni aniqlash uchun ishlatiladi.
- **Cookie-fayllar**: tizimga kirganingizda sessiya cookie-fayli oʻrnatiladi. U faqat shaffof boʻlmagan sessiya identifikatorini saqlaydi; biz cookie-fayllarni reklama yoki saytlararo kuzatish uchun ishlatmaymiz.

Biz shaxsiy maʼlumotlaringizni sotmaymiz va ularni reklama beruvchilar bilan ulashmaymiz.

## Mochi maʼlumotlaringizni qanday boshqaradi (bu muhim)

Mochi yagona serverli platforma emas, **peer-to-peer (P2P) tarmogʻi**dir. Bu sizning maxfiyligingiz uchun muhim boʻlgan tarzda maʼlumotlaringiz oqimini oʻzgartiradi:

- Obunachilarga, abonentlarga yoki umumiy joylarga eʼlon qilgan kontentingiz oʻsha obunachilar yoki abonentlarni joylashtiradigan **boshqa Mochi serverlariga koʻchiriladi**. {{operator.name}} oʻzi boshqarmaydigan serverlardagi kontentingiz nusxalarini oʻchira olmaydi. Agar postni oʻchirsangiz, ushbu server postni avval yuborgan peerlariga oʻchirish bildirishnomasini joʻnatadi, ammo oʻchirish imkon qadar bajariladi va oʻsha peerlarning hamkorligiga bogʻliq.
- Agar ommaviy profilni tanlasangiz, **shaxsingiz (foydalanuvchi nomi, profil, ochiq kalitlar)** Mochi katalogida eʼlon qilinadi, shunda boshqa Mochi foydalanuvchilari sizni topishi va siz bilan bogʻlanishi mumkin.
- Toʻgʻridan-toʻgʻri, shaxsiy kontent (qoralamalar, shaxsiy yozuvlar, muayyan oluvchilar oʻrtasidagi shaxsiy xabarlar) faqat ushbu serverda va oluvchilar serverlarida qoladi; u ommaviy P2P tarmogʻining qismi emas.
- Ushbu server sizga **boshqa tarmoqlarga ulanish** imkonini bergan hollarda – boshqa xizmatdagi hisobingizni bogʻlash orqali yoki bu yerda boshqa tarmoqlardagi odamlar kuzatishi yoki unga xabar yuborishi mumkin boʻlgan hisobni joylashtirish orqali – oʻsha hisob orqali eʼlon qilgan yoki yuborgan narsalaringiz oʻsha tarmoqlardagi serverlarga boradi. Ular Mochi serverlari emas va {{operator.name}} ularni boshqarmaydi. Bunday hisob oʻz tarmogʻida ommaviy boʻladi, baʼzi tarmoqlarda esa siz kuzatayotgan hisoblar va siz layk bosgan postlar ham ommaviy boʻladi. Keyinchalik biror narsani tahrirlash yoki oʻchirish oʻsha serverlar eʼtiborsiz qoldirishi mumkin boʻlgan soʻrovni yuboradi.

Qisqasi: Mochida yoki boshqa tarmoqlarda boshqalar bilan ulashishni tanlagan har qanday narsa, dizayn boʻyicha, {{operator.name}} nazoratidan tashqariga chiqadi. Eʼlon qilgan kontentingizga elektron pochta yoki boshqa federativ aloqaga munosabat bildirgandek munosabatda boʻling.

## Uchinchi tomonlar

Tegishli funksiyalardan foydalanganingizda quyidagi uchinchi tomonlar bilan zarur boʻlgan minimal shaxsiy maʼlumotni ulashamiz:

- **Elektron pochta xizmati**: chiquvchi xatlar (kirish kodlari, manzilni tasdiqlash, bildirishnomalar) {{operator.name}} tomonidan sozlangan SMTP provayderi orqali yuboriladi.
- **Uchinchi tomon kirish provayderlari**: agar Google, GitHub, Microsoft, Facebook yoki boshqa OAuth provayderi orqali kirsangiz, oʻsha provayder ushbu serverga kirganingizni biladi va elektron pochta manzilingiz hamda asosiy profil maʼlumotlaringizni biz bilan ulashadi.
- **Toʻlov provayderlari**: agar bozor ilovasi orqali xarid qilsangiz, toʻlov Stripe (yoki {{operator.name}} sozlagan toʻlov provayderi) tomonidan qayta ishlanadi. Biz toʻliq karta tafsilotlarini saqlamaymiz.
- **Boshqa tarmoqlar**: agar boshqa xizmatdagi hisobingizni bogʻlasangiz, oʻsha xizmat siz u orqali joylashtirgan narsalarni oladi va ushbu server oʻziga u yerda siz nomingizdan harakat qilish imkonini beradigan kirish maʼlumotlarini saqlaydi. Agar bu yerda boshqa tarmoq uchun hisob joylashtirsangiz, oʻsha tarmoqning serverlari va xizmatlari siz unga eʼlon qilgan narsalarni hisobning ommaviy manzili bilan birga oladi.

## Sizning huquqlaringiz

Yashash joyingizga qarab, shaxsiy maʼlumotlaringizga kirish, ularni tuzatish, eksport qilish, cheklash yoki oʻchirish huquqiga ega boʻlishingiz mumkin. Maʼlumotlaringizni eksport qilish va hisobingizni yopishni Sozlamalarda oʻzingiz bajarishingiz mumkin. Hisobingizni yopish uni imtiyozli muddatga bloklaydi, bu muddat davomida tizimga kirib, yopishni bekor qilishingiz mumkin; shundan soʻng hisob va uning maʼlumotlari ushbu serverdan olib tashlanadi (zaxira nusxalarini saqlash muddatlari va muayyan yozuvlarni saqlash boʻyicha har qanday huquqiy majburiyatga binoan). Boshqa har qanday huquqni amalga oshirish uchun yoki tizimga kira olmasangiz, **{{operator.email}}** manziliga elektron xat yuboring.

## Maʼlumotlarni saqlash

Hisob maʼlumotlaringizni hisobingiz faol boʻlguncha va yopilgandan keyin tasodifiy oʻchirishlardan tiklash hamda huquqiy va audit majburiyatlarini bajarish uchun maʼqul muddat davomida saqlaymiz. Zaxira nusxalari odatda 90 kungacha saqlanadi.

## Biz qayerdamiz

Ushbu xizmat **{{operator.jurisdiction}}**dan boshqariladi va ushbu serverda shaxsiy maʼlumotlaringizni qayta ishlashga {{operator.jurisdiction}} qonunchiligi qoʻllaniladi.

## Oʻzgarishlar

Ushbu shartlarni vaqti-vaqti bilan yangilab turishimiz mumkin. Muhim oʻzgarishlar kuchga kirishidan oldin eʼlon qilinadi. Oʻzgarishdan keyin xizmatdan foydalanishni davom ettirishingiz yangilangan shartlarni qabul qilganingizni anglatadi.

## Aloqa

Maxfiylikka oid har qanday savol uchun yoki soʻrov bildirish uchun: **{{operator.email}}**
