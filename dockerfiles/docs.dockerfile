FROM nginx:alpine AS runtime
COPY ./target/doc /usr/share/nginx/html
COPY ./docs/nginx.conf /etc/nginx/nginx.conf
EXPOSE 8080
