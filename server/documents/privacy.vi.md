# Quyền riêng tư Mochi

Thông báo về quyền riêng tư này giải thích cách **{{operator.name}}** (đơn vị vận hành máy chủ Mochi này) thu thập, sử dụng và chia sẻ dữ liệu cá nhân của bạn khi bạn tạo tài khoản hoặc sử dụng dịch vụ. {{operator.name}} là bên kiểm soát dữ liệu.

## Những gì chúng tôi thu thập

Khi bạn đăng ký và sử dụng máy chủ này, chúng tôi thu thập:

- **Dữ liệu tài khoản**: tên người dùng, các phương thức xác thực bạn chọn (địa chỉ email, passkey, ứng dụng xác thực, mã khôi phục, hoặc tài khoản từ nhà cung cấp đăng nhập bên thứ ba), và bất kỳ thông tin hồ sơ nào bạn thêm vào (tên hiển thị, ảnh đại diện, tiểu sử).
- **Nội dung bạn tạo**: các bài đăng, tin nhắn, bình luận, tệp, và bất kỳ dữ liệu nào khác bạn chọn xuất bản qua các ứng dụng được cài trên máy chủ này.
- **Dữ liệu kỹ thuật**: địa chỉ IP, thông tin trình duyệt/thiết bị, và dấu thời gian cho các yêu cầu bạn thực hiện. Dữ liệu này được dùng để giữ cho dịch vụ an toàn và hoạt động ổn định, và để phát hiện hành vi lạm dụng.
- **Cookie**: một cookie phiên được đặt khi bạn đăng nhập. Nó chỉ lưu một mã định danh phiên không thể đọc được; chúng tôi không sử dụng cookie cho mục đích quảng cáo hay theo dõi xuyên trang.

Chúng tôi không bán dữ liệu cá nhân của bạn và không chia sẻ nó với các nhà quảng cáo.

## Cách Mochi xử lý dữ liệu của bạn (điều này quan trọng)

Mochi là một **mạng ngang hàng (P2P)**, không phải một nền tảng máy chủ đơn lẻ. Điều đó thay đổi cách dữ liệu của bạn lưu chuyển theo những phương thức quan trọng đối với quyền riêng tư của bạn:

- Nội dung bạn xuất bản cho người theo dõi, người đăng ký hoặc các không gian được chia sẻ sẽ **được nhân bản đến các máy chủ Mochi khác** đang lưu trữ những người theo dõi hoặc đăng ký đó. {{operator.name}} không thể xoá các bản sao nội dung của bạn khỏi những máy chủ mà mình không vận hành. Nếu bạn xoá một bài đăng, máy chủ này sẽ gửi thông báo xoá đến các peer mà nó đã từng gửi bài đăng tới, nhưng việc xoá là theo nỗ lực tốt nhất và phụ thuộc vào sự phối hợp của các peer đó.
- **Danh tính của bạn (tên người dùng, hồ sơ, khoá công khai)** sẽ được công bố trong thư mục Mochi nếu bạn chọn hồ sơ công khai, để những người dùng Mochi khác có thể tìm và liên hệ với bạn.
- Nội dung trực tiếp, riêng tư (bản nháp, ghi chú riêng tư, tin nhắn riêng giữa những người nhận cụ thể) chỉ được lưu trên máy chủ này và máy chủ của người nhận; nội dung đó không thuộc về mạng P2P công khai.

Tóm lại: bất cứ điều gì bạn chọn chia sẻ với người khác trên Mochi sẽ vượt ra ngoài tầm kiểm soát của {{operator.name}}, theo thiết kế. Hãy coi nội dung bạn xuất bản như email hay bất kỳ hình thức truyền thông liên kết nào khác.

## Bên thứ ba

Chúng tôi chỉ chia sẻ tối thiểu dữ liệu cá nhân cần thiết với các bên thứ ba sau khi bạn sử dụng các tính năng liên quan:

- **Dịch vụ email**: các email gửi đi (mã đăng ký, thông báo, đặt lại mật khẩu) được gửi qua một nhà cung cấp SMTP do {{operator.name}} cấu hình.
- **Nhà cung cấp đăng nhập bên thứ ba**: nếu bạn đăng nhập bằng Google, GitHub, Microsoft, Facebook hoặc nhà cung cấp OAuth khác, nhà cung cấp đó sẽ biết rằng bạn đã đăng nhập vào máy chủ này, và chia sẻ địa chỉ email cùng dữ liệu hồ sơ cơ bản của bạn với chúng tôi.
- **Nhà cung cấp thanh toán**: nếu bạn thực hiện giao dịch mua qua một ứng dụng marketplace, khoản thanh toán được xử lý bởi Stripe (hoặc nhà cung cấp thanh toán mà {{operator.name}} đã cấu hình). Chúng tôi không lưu trữ đầy đủ thông tin thẻ.

## Quyền của bạn

Tuỳ thuộc vào nơi bạn cư trú, bạn có thể có các quyền truy cập, chỉnh sửa, xuất, hạn chế hoặc xoá dữ liệu cá nhân của mình. Để thực hiện bất kỳ quyền nào trong số đó - bao gồm việc đóng tài khoản và yêu cầu xoá dữ liệu khỏi máy chủ này - hãy gửi email đến **{{operator.email}}**. Hiện tại, việc đóng tài khoản do nhà điều hành máy chủ xử lý theo yêu cầu; sau khi nhận email của bạn, chúng tôi sẽ đóng tài khoản và xoá dữ liệu liên quan khỏi máy chủ này (tuỳ thuộc vào việc lưu giữ bản sao lưu và bất kỳ nghĩa vụ pháp lý nào về việc giữ lại các hồ sơ cụ thể).

## Lưu giữ dữ liệu

Chúng tôi giữ dữ liệu tài khoản của bạn trong suốt thời gian tài khoản còn hoạt động, và trong một khoảng thời gian hợp lý sau khi đóng để khôi phục từ việc xoá ngoài ý muốn và để đáp ứng các nghĩa vụ pháp lý và kiểm toán. Bản sao lưu thường được giữ tối đa 90 ngày.

## Chúng tôi ở đâu

Dịch vụ này được vận hành từ **{{operator.jurisdiction}}**, và luật của {{operator.jurisdiction}} áp dụng cho việc xử lý dữ liệu cá nhân của bạn trên máy chủ này.

## Thay đổi

Chúng tôi có thể cập nhật các điều khoản này theo thời gian. Những thay đổi quan trọng sẽ được thông báo trước khi có hiệu lực. Việc tiếp tục sử dụng dịch vụ sau khi có thay đổi đồng nghĩa với việc bạn chấp nhận các điều khoản đã cập nhật.

## Liên hệ

Đối với bất kỳ câu hỏi nào về quyền riêng tư hoặc để gửi yêu cầu: **{{operator.email}}**
