package com.octopus.mystique.web.servlet;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

public class DispatcherServlet extends HttpServlet {

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    super.service(req, resp);
  }

  private void doDispatcher(HttpServletRequest req, HttpServletResponse resp){
    String requestURI = req.getRequestURI();
  }
}
