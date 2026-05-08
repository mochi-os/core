# Mochi Integritet

Denna integritetsinformation förklarar hur **{{operator.name}}** (operatören av denna Mochi-server) samlar in, använder och delar dina personuppgifter när du skapar ett konto eller använder tjänsten. {{operator.name}} är personuppgiftsansvarig.

## Vad vi samlar in

När du registrerar dig och använder denna server samlar vi in:

- **Kontouppgifter**: ett användarnamn, de autentiseringsmetoder du väljer (e-postadress, passkeys, autentiseringsapp, återställningskoder eller ett konto från en inloggningsleverantör från tredje part) samt all profilinformation du lägger till (visningsnamn, avatar, biografi).
- **Innehåll du skapar**: inlägg, meddelanden, kommentarer, filer och andra uppgifter som du väljer att publicera via de appar som är installerade på denna server.
- **Tekniska uppgifter**: IP-adress, webbläsar-/enhetsinformation och tidsstämplar för de förfrågningar du gör. Detta används för att hålla tjänsten säker och i drift och för att upptäcka missbruk.
- **Cookies**: en sessionscookie sätts när du loggar in. Den lagrar endast en ogenomskinlig sessionsidentifierare; vi använder inte cookies för annonsering eller spårning över webbplatser.

Vi säljer inte dina personuppgifter och vi delar dem inte med annonsörer.

## Hur Mochi hanterar dina uppgifter (detta är viktigt)

Mochi är ett **peer-to-peer-nätverk (P2P)**, inte en plattform med en enskild server. Det förändrar hur dina uppgifter flödar på sätt som har betydelse för din integritet:

- Innehåll som du publicerar till följare, prenumeranter eller delade utrymmen **replikeras till andra Mochi-servrar** som värd för dessa följare eller prenumeranter. {{operator.name}} kan inte ta bort kopior av ditt innehåll från servrar som vi inte driver. Om du tar bort ett inlägg kommer denna server att skicka ett borttagningsmeddelande till de peers som inlägget tidigare har skickats till, men borttagningen sker enligt bästa förmåga och beror på samarbetet med dessa peers.
- Din **identitet (användarnamn, profil, publika nycklar)** publiceras i Mochi-katalogen om du väljer en publik profil, så att andra Mochi-användare kan hitta och kontakta dig.
- Direkt, privat innehåll (utkast, privata anteckningar, privata meddelanden mellan specifika mottagare) stannar endast på denna server och mottagarnas servrar; det är inte en del av det publika P2P-nätverket.

Kort sagt: allt du väljer att dela med andra människor på Mochi kommer att färdas bortom {{operator.name}}s kontroll, av designprincip. Behandla innehåll du publicerar som du skulle behandla e-post eller någon annan federerad kommunikation.

## Tredje parter

Vi delar minimum av personuppgifter som krävs med följande tredje parter när du använder de relaterade funktionerna:

- **E-posttjänst**: utgående e-post (registreringskoder, notiser, lösenordsåterställning) skickas via en SMTP-leverantör som konfigurerats av {{operator.name}}.
- **Inloggningsleverantörer från tredje part**: om du loggar in med Google, GitHub, Microsoft, Facebook eller en annan OAuth-leverantör får den leverantören veta att du loggade in på denna server och delar din e-postadress och grundläggande profiluppgifter med oss.
- **Betalleverantörer**: om du gör ett köp via en marknadsplatsapp behandlas betalningen av Stripe (eller den betalleverantör som {{operator.name}} har konfigurerat). Vi lagrar inte fullständiga kortuppgifter.

## Dina rättigheter

Beroende på var du bor kan du ha rätt att få tillgång till, korrigera, exportera, begränsa eller radera dina personuppgifter. För att utöva någon av dessa rättigheter - inklusive att stänga ditt konto och få dina uppgifter borttagna från denna server - skicka e-post till **{{operator.email}}**. Stängning av konton hanteras för närvarande av serveroperatören på begäran; när vi har mottagit din e-post stänger vi kontot och tar bort tillhörande uppgifter från denna server (med förbehåll för säkerhetskopieringsretention och eventuella rättsliga skyldigheter att bevara specifika uppgifter).

## Uppgiftslagring

Vi behåller dina kontouppgifter så länge ditt konto är aktivt och under en rimlig period efter stängning för att kunna återhämta oss från oavsiktlig borttagning och uppfylla rättsliga och revisionsmässiga skyldigheter. Säkerhetskopior bevaras vanligtvis i upp till 90 dagar.

## Var vi finns

Denna tjänst drivs från **{{operator.jurisdiction}}**, och lagen i {{operator.jurisdiction}} gäller för behandlingen av dina personuppgifter på denna server.

## Ändringar

Vi kan uppdatera dessa villkor då och då. Väsentliga ändringar kommer att tillkännages innan de träder i kraft. Fortsatt användning av tjänsten efter en ändring innebär att du accepterar de uppdaterade villkoren.

## Kontakt

För integritetsfrågor eller för att göra en begäran: **{{operator.email}}**
