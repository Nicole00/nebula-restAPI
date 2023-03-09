package com.vesoft.nebula.graph.server.controller;

import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.graph.server.entity.ErrorCode;
import com.vesoft.nebula.graph.server.entity.NebulaConnectRequest;
import com.vesoft.nebula.graph.server.entity.NebulaConnectResponse;
import com.vesoft.nebula.graph.server.entity.NebulaQueryRequest;
import com.vesoft.nebula.graph.server.entity.NebulaQueryResponse;
import com.vesoft.nebula.graph.server.entity.NebulaQueryResult;
import com.vesoft.nebula.graph.server.exceptions.ConnectionException;
import com.vesoft.nebula.graph.server.exceptions.NullObjectException;
import com.vesoft.nebula.graph.server.service.NebulaGraphService;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GraphQueryController {
    private static Logger LOG = LoggerFactory.getLogger(GraphQueryController.class);

    @Autowired
    NebulaGraphService nebulaGraphService;

    /**
     * connect NebulaGraph
     *
     * @param request nebula connection parameter
     * @return CommonResponse
     */
    @PostMapping("/graph-compute/connect")
    @ResponseBody
    public NebulaConnectResponse connectNebulaGraph(@RequestBody NebulaConnectRequest request,
                                                    HttpServletResponse cookieResp) {
        LOG.info("enter NebulaGraphController.connectNebulaGraph, parameters: {}",
                request.toString());

        NebulaConnectResponse response = new NebulaConnectResponse();
        String host = request.getHost();
        String user = request.getUsername();
        String passwd = request.getPassword();

        if (host == null || host.trim().isEmpty()) {
            LOG.error("host is invalid, your host is {}", host);
            response.setResp(ErrorCode.INVALID_CONNECTION_PARAMETER);
            return response;
        }
        if (user == null || user.trim().isEmpty() || passwd == null || passwd.trim().isEmpty()) {
            LOG.error("user or password is invalid, your user is {},password is {}", user, passwd);
            response.setResp(ErrorCode.INVALID_CONNECTION_PARAMETER);
            return response;
        }

        String[] hosts = host.split(",");
        for (String address : hosts) {
            String[] ipAndPort = address.split(":");
            if (ipAndPort.length < 2) {
                LOG.error("host {} has wrong format", host);
                response.setResp(ErrorCode.INVALID_CONNECTION_PARAMETER);
                return response;
            }
            if (Integer.parseInt(ipAndPort[1]) <= 0 || Integer.parseInt(ipAndPort[1]) >= 65535) {
                LOG.error("port {} out of range.", ipAndPort[1]);
                response.setResp(ErrorCode.INVALID_CONNECTION_PARAMETER);
                return response;
            }
        }

        try {
            String sessionId = nebulaGraphService.connect(host, user, passwd);
            response.setData(sessionId);
            Cookie cookie = new Cookie("nsid", sessionId);
            cookie.setSecure(true);
            cookie.setMaxAge(24 * 60 * 60);
            cookie.setHttpOnly(true);
            cookieResp.addCookie(cookie);
        } catch (Exception e) {
            LOG.error("NebulaGraphController.connectNebulaGraph error: ", e);
            response.setResp(ErrorCode.CONNECT_ERROR);
            return response;
        }
        LOG.info("connectNebulaGraph response: {}", response.toString());
        return response;
    }

    /**
     * disconnect NebulaGraph
     */
    @PostMapping("/graph-compute/disconnect")
    public NebulaConnectResponse disconnectNebulaGraph(
            @CookieValue(name = "nsid", defaultValue = "") String nsid) {
        LOG.info("enter NebulaGraphController.disconnectNebulaGraph");

        NebulaConnectResponse response = new NebulaConnectResponse();

        if (nsid == null || "".equals(nsid)) {
            LOG.error("disconnect failed for lack of session");
            response.setResp(ErrorCode.NOT_CONNECT);
            return response;
        }
        try {
            nebulaGraphService.disconnect(nsid);
        } catch (Exception e) {
            LOG.error("NebulaGraphController.disconnectNebulaGraph error, ", e);
            response.setResp(ErrorCode.ERROR);
            return response;
        }
        return response;
    }


    /**
     * execute ngql
     *
     * @param request
     * @return NebulaQueryResponse
     */
    @PostMapping("/graph-compute/executeNGQL")
    @ResponseBody
    public synchronized NebulaQueryResponse executeNGQL(
            @CookieValue(name = "nsid", defaultValue = "") String nsid,
            @RequestBody NebulaQueryRequest request) {
        LOG.info("enter NebulaGraphController.executeNGQL, parameters:{}", request.toString());
        NebulaQueryResponse response = new NebulaQueryResponse();

        if (nsid == null || "".equals(nsid)) {
            LOG.error("connect failed for lack of session");
            response.setResp(ErrorCode.NOT_CONNECT);
            return response;
        }
        String ngql = request.getGql();

        if (ngql == null || "".equals(ngql.trim())) {
            LOG.error("ngql is null");
            response.setResp(ErrorCode.EMPTY_NGQL);
            return response;
        }
        NebulaQueryResult result = null;
        try {
            result = nebulaGraphService.executeNgql(nsid, ngql);
        } catch (NullObjectException e) {
            LOG.error("NebulaGraphController.executeNGQL error: ", e);
            response.setResp(ErrorCode.NOT_CONNECT);
        } catch (ConnectionException e) {
            LOG.error("NebulaGraphController.executeNGQL error: ", e);
            response.setResp(ErrorCode.CONNECT_ERROR);
        } catch (IOErrorException e) {
            LOG.error("NebulaGraphController.executeNGQL error: ", e);
            if (e.getType() == IOErrorException.E_CONNECT_BROKEN || e.getType() == IOErrorException.E_UNKNOWN) {
                response.setResp(ErrorCode.INVALID_CONNECT);
            } else {
                response.setResp(ErrorCode.CONNECT_ERROR);
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("NebulaGraphController.executeNGQL error: ", e);
            response.setCode(ErrorCode.ENCODE_ERROR.getErrorCode());
            response.setMessage(e.getMessage());
        } catch (Exception e) {
            LOG.error("NebulaGraphController.executeNGQL error: ", e);
            response.setCode(ErrorCode.ERROR.getErrorCode());
            response.setMessage(e.getMessage());
        }
        if (result == null) {
            LOG.error("ngql " + ngql + ", result is null");
            return response;
        }

        response.setData(result);
        response.setCode(result.getErrorCode());
        if (result.getErrorMessage() != null && !"".equals(result.getErrorMessage())) {
            response.setMessage(result.getErrorMessage());
        }
        LOG.info("executeNGQL response: {}", response.toString());
        return response;
    }

}
