using LuoliCommon.DTO.Coupon;
using LuoliCommon.DTO.ExternalOrder;
using LuoliCommon.Entities;
using LuoliCommon.Enums;

namespace CouponService
{
    public interface ICouponRepo
    {

        Task<ApiResponse<CouponDTO>> GenerateAsync(ExternalOrderDTO dto);
        Task<ApiResponse<CouponDTO>> GenerateManualAsync(string from_platform, string tid, decimal amount);
        
        Task<ApiResponse<CouponDTO>> GetAsync(string coupon);
        Task<ApiResponse<CouponDTO>> GetByTidAsync(string platform, string tid);
        Task<ApiResponse<PageResult<CouponDTO>>> GetAsync( int page = 1,
            int size = 10,
            // 可添加更多筛选条件，如订单状态、时间范围等
            byte? couponStatus = null,
            DateTime? startTime = null,
            DateTime? endTime = null);

        Task<ApiResponse<IEnumerable<CouponDTO>>> PersonalCouponsAsync(string coupon,
            string targetProxy,
            DateTime? from,
            DateTime? to,
            int? limit);

        Task<ApiResponse<List<CouponDTO>>> GetAsync(string[] coupons, ECouponStatus? status = null);
        
        Task<ApiResponse<bool>> UpdateAsync(CouponDTO dto);
        Task<ApiResponse<bool>> DeleteAsync(string coupon );


    }
}
