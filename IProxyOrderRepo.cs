using LuoliCommon.DTO.Coupon;
using LuoliCommon.DTO.ExternalOrder;
using LuoliCommon.DTO.ProxyOrder;
using LuoliCommon.Entities;
using LuoliCommon.Enums;

namespace CouponService
{
    public interface IProxyOrderRepo
    {
        Task<ApiResponse<ProxyOrderDTO>> GetAsync(string coupon);
        Task<ApiResponse<IEnumerable<ProxyOrderDTO>>> GetAsync(string targetProxy, string[] coupons, string? orderStatus = null);
        Task<ApiResponse<bool>> UpdateAsync(ProxyOrderDTO dto);
        Task<ApiResponse<bool>> DeleteAsync(string targetProxy, string proxyOrderId);
        Task<ApiResponse<bool>> InsertAsync(ProxyOrderDTO dto);


    }
}
