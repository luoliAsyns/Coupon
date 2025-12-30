using System.Text.Json.Nodes;
using LuoliCommon.DTO.Coupon;
using LuoliCommon.DTO.ExternalOrder;
using LuoliCommon.Entities;
using LuoliCommon.Enums;
using LuoliCommon.Interfaces;
using LuoliCommon.Logger;
using LuoliDatabase.Extensions;
using MethodTimer;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using ILogger = LuoliCommon.Logger.ILogger;

namespace CouponService.Controllers;

public class CouponController : Controller, ICouponService
{
    private readonly ILogger _logger;
    private readonly ICouponRepo _couponRepo;

    public CouponController(ILogger logger, ICouponRepo couponRepo)
    {
        _logger = logger;
        _couponRepo = couponRepo;
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
            response = await _couponRepo.GetAsync(coupon);
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
    public async Task<ApiResponse<CouponDTO>> Query(
       [FromQuery] string tid,
       [FromQuery] string from_platform)
    {
        _logger.Info($"trigger CouponService.Controllers.Query tid:[{tid}]");

        ApiResponse<CouponDTO> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        try
        {
            response = await _couponRepo.GetByTidAsync(from_platform, tid);
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
        [FromQuery] ECouponStatus status = ECouponStatus.Default)
    {
        _logger.Info($"trigger CouponService.Controllers.Validate coupons:[{string.Join(",", coupons)}]");

        ApiResponse<List<CouponDTO>> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

        try
        {
            if (status != ECouponStatus.Default)
                response = await _couponRepo.GetAsync(coupons, status);
            else
                response = await _couponRepo.GetAsync(coupons, null);
                
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
    public async Task<ApiResponse<bool>> Invalidate([FromBody] UpdateErrorCodeRequest fromBody)
    {
        string coupon = fromBody.Coupon;
        _logger.Info($"trigger CouponService.Controllers.Invalidate coupon:[{coupon}]");

        ApiResponse<bool> response = new();
        response.code = EResponseCode.Fail;
        response.data = false;

        try
        {
            var couponDto =(await _couponRepo.GetAsync(coupon)).data;

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
            response = await _couponRepo.GetAsync(page, size, status, from, to );
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
            response = await _couponRepo.PersonalCouponsAsync(coupon, targetProxy, from, to , limit);
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
            response = await _couponRepo.GenerateAsync(dto);
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
        [FromBody] GenerateManualReqest jObject)
    {

        string from_platform = jObject.from_platform;
        string tid = jObject.tid;
        decimal amount = jObject.amount;


        _logger.Info($"trigger CouponService.Controllers.GenerateManual");

        ApiResponse<CouponDTO> response = new();
        response.code = EResponseCode.Fail;
        response.data = null;

      
        try
        {
            response = await _couponRepo.GenerateManualAsync(from_platform, tid, amount);
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
            response = await _couponRepo.DeleteAsync(coupon);
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
            response = await _couponRepo.UpdateAsync(dto);
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
            response = await _couponRepo.UpdateAsync(resp.data);
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