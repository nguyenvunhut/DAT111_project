-- Kiểm tra và xóa stored procedure nếu nó đã tồn tại
IF OBJECT_ID('dbo.CreateDatabaseDynamically') IS NOT NULL
    DROP PROCEDURE dbo.CreateDatabaseDynamically;
GO

-- Tạo stored procedure
CREATE PROCEDURE dbo.CreateDatabaseDynamically
    @DatabaseName NVARCHAR(128) -- Biến chứa tên database bạn muốn tạo
AS
BEGIN
    -- Ngăn chặn số lượng hàng bị ảnh hưởng hiển thị
    SET NOCOUNT ON;

    -- Kiểm tra xem tên database có hợp lệ (không NULL và không rỗng)
    IF @DatabaseName IS NULL OR @DatabaseName = ''
    BEGIN
        -- Gửi thông báo lỗi nếu tên database không hợp lệ
        RAISERROR(N'Tên database không được để trống.', 16, 1);
        RETURN;
    END

    -- Xây dựng câu lệnh SQL động
    DECLARE @SQL nvarchar(MAX);
    -- Đảm bảo tên database được đặt trong dấu ngoặc vuông [] để xử lý các tên có ký tự đặc biệt
    SET @SQL = N'CREATE DATABASE ' + QUOTENAME(@DatabaseName) + N';';

    -- 

    -- Thực thi câu lệnh SQL động
    EXEC sp_executesql @SQL;

    -- Thông báo thành công (tùy chọn)
    PRINT N'Database ' + QUOTENAME(@DatabaseName) + N' đã được tạo thành công.';
END
GO

-- 1. Khai báo biến và gán tên database
DECLARE @NewDBName NVARCHAR(128) = N'HeartDiseaseDB';

-- 2. Thực thi stored procedure với biến đó
EXEC dbo.CreateDatabaseDynamically @DatabaseName = @NewDBName;
