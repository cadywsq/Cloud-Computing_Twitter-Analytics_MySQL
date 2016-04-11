package TeamProj.main;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import query1.Q1Servlet;
import query2.Q2Servlet;
import query3.Q3Servlet;
import query4.Q4Servlet;

import javax.servlet.ServletException;

import static io.undertow.servlet.Servlets.defaultContainer;
import static io.undertow.servlet.Servlets.deployment;
import static io.undertow.servlet.Servlets.servlet;

public class Main {
    public Main() throws Exception {

    }

    public static final String PATH = "/";

    public static void main(String[] args) throws Exception {
        try {
            DeploymentInfo servletBuilder = deployment()
                    .setClassLoader(Main.class.getClassLoader())
                    .setContextPath(PATH)
                    .setDeploymentName("handler.war")
                    .addServlets(
                            servlet("Q1Servlet", Q1Servlet.class).addMapping("/query1"),
                            servlet("Q2Servlet", Q2Servlet.class).addMapping("/query2"),
                            servlet("Q3Servlet", Q3Servlet.class).addMapping("/query3"),
                            servlet("Q4Servlet", Q4Servlet.class).addMapping("/query4")
                    );

            DeploymentManager manager = defaultContainer().addDeployment(servletBuilder);
            manager.deploy();

            HttpHandler servletHandler = manager.start();
            PathHandler path = Handlers.path(Handlers.redirect(PATH))
                    .addPrefixPath(PATH, servletHandler);

            Undertow server = Undertow.builder()
                    .addHttpListener(80, "0.0.0.0")
                    .setHandler(path)
                    .build();
            server.start();
        } catch (ServletException e) {
            throw new RuntimeException(e);
        }
    }
}
