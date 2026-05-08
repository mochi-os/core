# Mochi məxfilik bildirişi

Bu məxfilik bildirişi **{{operator.name}}**-in (bu Mochi serverinin operatoru) hesab yaratdığınız və ya xidmətdən istifadə etdiyiniz zaman şəxsi məlumatlarınızı necə topladığını, istifadə etdiyini və paylaşdığını izah edir. {{operator.name}} məlumat nəzarətçisidir.

## Nə topladığımız

Bu serverə qoşulduğunuz və istifadə etdiyiniz zaman aşağıdakıları toplayırıq:

- **Hesab məlumatları**: istifadəçi adı, seçdiyiniz autentifikasiya üsulları (e-poçt ünvanı, passkey-lər, autentifikator tətbiqi, bərpa kodları və ya üçüncü tərəf giriş təminatçısının hesabı) və əlavə etdiyiniz hər hansı profil məlumatı (göstərilən ad, avatar, bioqrafiya).
- **Yaratdığınız məzmun**: bu serverdə quraşdırılmış tətbiqlər vasitəsilə dərc etməyi seçdiyiniz yazılar, mesajlar, şərhlər, fayllar və hər hansı digər məlumat.
- **Texniki məlumatlar**: IP ünvanı, brauzer/cihaz məlumatları və göndərdiyiniz sorğuların vaxt nişanları. Bu, xidmətin təhlükəsiz və işlək qalması, habelə sui-istifadənin aşkarlanması üçün istifadə olunur.
- **Kuki-lər**: daxil olduğunuz zaman sessiya kukisi qoyulur. O, yalnız anlaşılmaz sessiya identifikatorunu saxlayır; biz kukiləri reklam və ya saytlararası izləmə üçün istifadə etmirik.

Biz şəxsi məlumatlarınızı satmırıq və onları reklamçılarla paylaşmırıq.

## Mochi məlumatlarınızı necə idarə edir (bu vacibdir)

Mochi vahid serverli platforma deyil, **peer-to-peer (P2P) şəbəkəsidir**. Bu, məxfiliyiniz üçün əhəmiyyətli olan şəkildə məlumatlarınızın necə hərəkət etdiyini dəyişir:

- İzləyicilərə, abunəçilərə və ya paylaşılan məkanlara dərc etdiyiniz məzmun həmin izləyiciləri və ya abunəçiləri yerləşdirən **digər Mochi serverlərinə təkrarlanır**. {{operator.name}} özünün idarə etmədiyi serverlərdən məzmununuzun nüsxələrini silə bilməz. Bir yazını silsəniz, bu server əvvəllər həmin yazını göndərdiyi peer-lərə silmə bildirişi göndərəcək, lakin silmə əməliyyatı mümkün olan ən yaxşı səviyyədədir və həmin peer-lərin əməkdaşlığından asılıdır.
- Açıq profil seçsəniz, **kimliyiniz (istifadəçi adı, profil, açıq açarlar)** Mochi kataloquna dərc olunur ki, digər Mochi istifadəçiləri sizi tapıb sizinlə əlaqə saxlaya bilsin.
- Birbaşa, şəxsi məzmun (qaralamalar, şəxsi qeydlər, müəyyən alıcılar arasında şəxsi mesajlar) yalnız bu serverdə və alıcıların serverlərində qalır; o, açıq P2P şəbəkəsinin bir hissəsi deyil.

Qısaca: Mochi-də digər insanlarla paylaşmağı seçdiyiniz hər şey, dizayn etibarilə, {{operator.name}}-in nəzarətindən kənara çıxacaq. Dərc etdiyiniz məzmuna e-poçt və ya hər hansı digər federasiyalaşmış kommunikasiya kimi yanaşın.

## Üçüncü tərəflər

Müvafiq xüsusiyyətlərdən istifadə etdiyiniz zaman aşağıdakı üçüncü tərəflərlə tələb olunan minimum şəxsi məlumatları paylaşırıq:

- **E-poçt xidməti**: gedən e-poçtlar (qeydiyyat kodları, bildirişlər, parol sıfırlama) {{operator.name}} tərəfindən konfiqurasiya edilmiş SMTP təminatçısı vasitəsilə göndərilir.
- **Üçüncü tərəf giriş təminatçıları**: Google, GitHub, Microsoft, Facebook və ya başqa OAuth təminatçısı ilə daxil olsanız, həmin təminatçı bu serverə daxil olduğunuzu öyrənir və e-poçt ünvanınızı və əsas profil məlumatlarınızı bizimlə paylaşır.
- **Ödəniş təminatçıları**: marketpleys tətbiqi vasitəsilə alış edirsinizsə, ödəniş Stripe (və ya {{operator.name}}-in konfiqurasiya etdiyi hər hansı ödəniş təminatçısı) tərəfindən emal olunur. Biz tam kart məlumatlarını saxlamırıq.

## Hüquqlarınız

Yaşadığınız yerdən asılı olaraq, şəxsi məlumatlarınıza giriş, onları düzəltmək, ixrac etmək, məhdudlaşdırmaq və ya silmək hüquqlarınız ola bilər. Bu hüquqlardan hər hansı birini həyata keçirmək üçün - hesabınızı bağlamaq və məlumatlarınızı bu serverdən silmək daxil olmaqla - **{{operator.email}}** ünvanına e-poçt göndərin. Hesabın bağlanması hazırda sorğu əsasında server operatoru tərəfindən həyata keçirilir; e-poçtunuzu aldıqdan sonra hesabı bağlayacaq və əlaqəli məlumatları bu serverdən siləcəyik (ehtiyat surət saxlanması və müəyyən qeydləri saxlamaq üçün hər hansı qanuni öhdəlik nəzərə alınmaqla).

## Məlumatların saxlanması

Hesab məlumatlarınızı hesabınız aktiv olduğu müddətdə və qəza ilə silinmədən bərpa olunmaq, habelə qanuni və audit öhdəliklərini yerinə yetirmək üçün bağlandıqdan sonra ağlabatan müddət ərzində saxlayırıq. Ehtiyat surətlər adətən 90 günə qədər saxlanılır.

## Harada olduğumuz

Bu xidmət **{{operator.jurisdiction}}** ərazisindən idarə olunur və bu serverdə şəxsi məlumatlarınızın emalına {{operator.jurisdiction}} qanunu tətbiq olunur.

## Dəyişikliklər

Bu şərtləri vaxtaşırı yeniləyə bilərik. Əhəmiyyətli dəyişikliklər qüvvəyə minməzdən əvvəl elan ediləcək. Dəyişiklikdən sonra xidmətdən istifadəyə davam etmək yenilənmiş şərtləri qəbul etdiyiniz mənasını verir.

## Əlaqə

Hər hansı məxfilik sualı və ya sorğu vermək üçün: **{{operator.email}}**
