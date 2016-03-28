package TeamProj.TeamProj;

import static io.undertow.servlet.Servlets.defaultContainer;
import static io.undertow.servlet.Servlets.deployment;
import static io.undertow.servlet.Servlets.servlet;

import javax.servlet.ServletException;

import Q1.Q1Servlet;
import Q2.Q2Servlet;
import Q3.Q3Servlet;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;

public class Main {
    public Main() throws Exception{

    }
    public static final String PATH = "/";
    public static void main(String[] args) throws Exception{
        try {
            System.out.println("vera");
            DeploymentInfo servletBuilder = deployment()
                    .setClassLoader(Main.class.getClassLoader())
                    .setContextPath(PATH)
                    .setDeploymentName("handler.war")
                    .addServlets(
                            servlet("Q1Servlet", Q1Servlet.class)
                            .addMapping("/q1"),
                            servlet("Q2Servlet", Q2Servlet.class)
                            .addMapping("/q2"),
                            servlet("Q3Servlet", Q3Servlet.class)
                            .addMapping("/q3")
                    );


            DeploymentManager manager = defaultContainer().addDeployment(servletBuilder);
            manager.deploy();

            HttpHandler servletHandler = manager.start();
            PathHandler path = Handlers.path(Handlers.redirect(PATH))
                    .addPrefixPath(PATH, servletHandler);

            Undertow server = Undertow.builder()
                    .addHttpListener(8080, "0.0.0.0")
                    .setHandler(path)
                    .build();
            server.start();
        } catch (ServletException e) {
            throw new RuntimeException(e);
        }
    }
}
