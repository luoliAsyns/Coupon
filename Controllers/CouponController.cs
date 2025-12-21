using System.Text.Json.Nodes;
using LuoliCommon.DTO.Coupon;
using LuoliCommon.DTO.ExternalOrder;
using LuoliCommon.Entities;
using LuoliCommon.Enums;
using LuoliCommon.Logger;
using LuoliDatabase.Extensions;
using MethodTimer;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using ILogger = LuoliCommon.Logger.ILogger;

namespace CouponService.Controllers;

public class CouponController : Controller
{
    private readonly ILogger _logger;
    private readonly ICouponService _couponService;

    public CouponController(ILogger logger, ICouponService couponService)
    {
        _logger = logger;
        _couponService = couponService;
    }


    [Time]
    [HttpGet]
    [Route("api/coupon/query-coupon")]
    public async Task<ApiResponse<CouponDTO>> Query(
        [FromQuery] string coupon)
    {
        _logger.Info($"trigger CouponService.Controllers.Query coupon:[{coupon}]");

        ApiResponse<CouponDTO> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        try
        {
            response = await _couponService.GetAsync(coupon);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.Query");
            _logger.Error(ex.Message);
        }

        return response;
    }
    [Time]
    [HttpGet]
    [Route("api/coupon/query-tid")]
    public async Task<ApiResponse<CouponDTO>> QueryWithTid(
       [FromQuery] string tid,
       [FromQuery] string from_platform)
    {
        _logger.Info($"trigger CouponService.Controllers.Query tid:[{tid}]");

        ApiResponse<CouponDTO> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        try
        {
            response = await _couponService.GetByTidAsync(from_platform, tid);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.Query");
            _logger.Error(ex.Message);
        }

        return response;
    }


    [Time]
    [HttpGet]
    [Route("api/coupon/validate")]
    public async Task<ApiResponse<List<CouponDTO>>> Validate(
        [FromQuery] string[] coupons,
        [FromQuery] byte? status)
    {
        _logger.Info($"trigger CouponService.Controllers.Validate coupons:[{string.Join(",", coupons)}]");

        ApiResponse<List<CouponDTO>> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        try
        {
            if (status.HasValue)
                response = await _couponService.GetAsync(coupons, (ECouponStatus)status.Value);
            else
                response = await _couponService.GetAsync(coupons, null);
                
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.Validate");
            _logger.Error(ex.Message);
        }

        return response;
    }

    
    [Time]
    [HttpPost]
    [Route("api/coupon/invalidate")]
    public async Task<ApiResponse<bool>> Invalidate([FromBody] JsonObject fromBody)
    {
        string coupon =fromBody["coupon"]?.ToString();
        _logger.Info($"trigger CouponService.Controllers.Invalidate coupon:[{coupon}]");

        ApiResponse<bool> response = new();
        response.code = EResponseCode.Fail;
        response.data = false;

        try
        {
            var couponDto =(await _couponService.GetAsync(coupon)).data;

            return await Update(new LuoliCommon.DTO.Coupon.UpdateRequest()
            {
                Coupon = couponDto,
                Event = EEvent.Receive_Manual_Cancel_Coupon
            });
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.Invalidate");
            _logger.Error(ex.Message);
        }

        return response;
    }
    
    [Time]
    [HttpGet]
    [Route("api/coupon/page-query")]
    public async Task<ApiResponse<PageResult<CouponDTO>>> PageQuery(
        [FromQuery] int page ,
        [FromQuery] int size ,
        [FromQuery] byte? status,
        [FromQuery] DateTime? from,
        [FromQuery] DateTime? to)
    {
        _logger.Info($"trigger CouponService.Controllers.PageQuery");

        ApiResponse<PageResult<CouponDTO>> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        try
        {
            response = await _couponService.GetAsync(page, size, status, from, to );
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.PageQuery");
            _logger.Error(ex.Message);
        }

        return response;
    }

    [Time]
    [HttpGet]
    [Route("api/coupon/personal-coupons")]
    public async Task<ApiResponse<IEnumerable<CouponDTO>>> PersonalCoupons(
       [FromQuery] string coupon,
       [FromQuery] string targetProxy,
       [FromQuery] DateTime? from,
       [FromQuery] DateTime? to,
       [FromQuery] int? limit)
    {
        _logger.Info($"trigger CouponService.Controllers.PersonalCoupons");

        ApiResponse<IEnumerable<CouponDTO>> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        try
        {
            response = await _couponService.PersonalCouponsAsync(coupon, targetProxy, from, to , limit);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.PersonalCoupons");
            _logger.Error(ex.Message);
        }

        return response;
    }




    [Time]
    [HttpPost]
    [Route("api/coupon/generate")]
    public async Task<ApiResponse<CouponDTO>> Generate(
        [FromBody] ExternalOrderDTO dto)
    {
        _logger.Info($"trigger CouponService.Controllers.Generate");

        ApiResponse<CouponDTO> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        var (valid, msg) = dto.Validate();
        if (!valid)
        {
            response.msg = msg;
            _logger.Error($"while CouponService.Controllers.Generate, not passed validate. msg:[{msg}]");
            return response;
        }

        try
        {
            response = await _couponService.GenerateAsync(dto);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.Generate");
            _logger.Error(ex.Message);
        }

        return response;
    }

    [Time]
    [HttpPost]
    [Route("api/coupon/generate-manual")]
    public async Task<ApiResponse<CouponDTO>> GenerateManual(
        [FromBody] dynamic jObject)
    {

        string from_platform = jObject.GetProperty("from_platform").GetString() ?? string.Empty;
        string tid = jObject.GetProperty("tid").GetString() ?? string.Empty;
        decimal amount = jObject.GetProperty("amount").GetDecimal();


        _logger.Info($"trigger CouponService.Controllers.GenerateManual");

        ApiResponse<CouponDTO> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

      
        try
        {
            response = await _couponService.GenerateManualAsync(from_platform, tid, amount);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.GenerateManual");
            _logger.Error(ex.Message);
        }

        return response;
    }
    
    [Time]
    [HttpPost]
    [Route("api/coupon/delete")]
    public async Task<ApiResponse<bool>> Delete(
        [FromBody] string coupon)
    {
        _logger.Info($"trigger CouponService.Controllers.Delete");

        ApiResponse<bool> response = new();
        response.code = EResponseCode.Fail;
        response.data = false;


        try
        {
            response = await _couponService.DeleteAsync(coupon);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.Delete");
            _logger.Error(ex.Message);
        }

        return response;
    }

    [Time]
    [HttpPost]
    [Route("api/coupon/update")]
    public async Task<ApiResponse<bool>> Update(
        [FromBody] LuoliCommon.DTO.Coupon.UpdateRequest ur)
    {
        _logger.Info($"trigger CouponService.Controllers.Update");

        var dto = ur.Coupon;

        ApiResponse<bool> response = new();
        response.code = EResponseCode.Fail;
        response.data = false;

        var (valid, msg) = dto.Validate();
        if (!valid)
        {
            response.msg = msg;
            _logger.Error($"while CouponService.Controllers.Update, not passed validate. msg:[{msg}]");
            return response;
        }


        var rawStatus = ur.Coupon.Status;

        var updateStatus = ur.UpdateStatus(ur.Coupon, ur.Event);
        if (!updateStatus)
        {
            response.msg = $"coupon:[{ur.Coupon.Coupon}] raw Status:[{rawStatus}] Event:[{ur.Event.ToString()}], not meet UpdateStatus condition";
            _logger.Error(response.msg);
            return response;
        }

        _logger.Info($"coupon:[{ur.Coupon.Coupon}] raw Status:[{rawStatus.ToString()}] Event:[{ur.Event.ToString()}] new Status:[{ur.Coupon.Status.ToString()}]");

        try
        {
            response = await _couponService.UpdateAsync(dto);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.Update");
            _logger.Error(ex.Message);
        }

        return response;
    }

    [Time]
    [HttpPost]
    [Route("api/coupon/update-error")]
    public async Task<ApiResponse<bool>> UpdateErrorCode(
        [FromBody] LuoliCommon.DTO.Coupon.UpdateErrorCodeRequest ur)
    {
        _logger.Info($"trigger CouponService.Controllers.UpdateErrorCode with coupon[{ur.Coupon}] errorCode[{ur.ErrorCode}]");

      

        ApiResponse<bool> response = new();
        response.code = EResponseCode.Fail;
        response.data = false;

        var resp = await Query(ur.Coupon);
        if (resp.data is null)
        {
            response.msg = $"coupon[{ur.Coupon}] not exist";
            return response;
        }

        try
        {
            resp.data.ErrorCode = ur.ErrorCode;
            response = await _couponService.UpdateAsync(resp.data);
        }
        catch (Exception ex)
        {
            response.msg = ex.Message;
            response.code = EResponseCode.Fail;

            _logger.Error("while CouponService.Controllers.UpdateErrorCode");
            _logger.Error(ex.Message);
        }

        return response;
    }

}